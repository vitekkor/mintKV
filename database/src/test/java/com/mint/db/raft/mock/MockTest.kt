package com.mint.db.raft.mock

import com.google.protobuf.ByteString
import com.google.protobuf.Message
import com.mint.db.Raft
import com.mint.db.Raft.AppendEntriesRequest
import com.mint.db.Raft.VoteRequest
import com.mint.db.config.NodeConfig
import com.mint.db.dao.impl.BaseDao
import com.mint.db.grpc.ExternalGrpcActorInterface
import com.mint.db.grpc.InternalGrpcActor
import com.mint.db.raft.DaoStateMachine
import com.mint.db.raft.Environment
import com.mint.db.raft.RaftActor
import com.mint.db.raft.StateMachine
import com.mint.db.raft.Timeout
import com.mint.db.raft.mock.ProcessAction.AppendLogEntry
import com.mint.db.raft.mock.ProcessAction.ApplyCommandLogEntry
import com.mint.db.raft.mock.ProcessAction.Result
import com.mint.db.raft.mock.ProcessAction.Send
import com.mint.db.raft.mock.ProcessAction.StartTimeout
import com.mint.db.raft.mock.ProcessAction.WritePersistentState
import com.mint.db.raft.model.Command
import com.mint.db.raft.model.CommandResult
import com.mint.db.raft.model.InsertCommand
import com.mint.db.raft.model.LogId
import com.mint.db.replication.ReplicatedLogManager
import com.mint.db.replication.model.LogEntry
import com.mint.db.replication.model.PersistentState
import com.mint.db.util.EntryConverter
import org.junit.After
import org.junit.Before
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.mockito.Mockito
import java.lang.foreign.MemorySegment
import kotlin.random.Random
import kotlin.random.nextInt
import kotlin.random.nextLong

sealed class ProcessAction {
    data class Send(val destId: Int, val message: Message) : ProcessAction()
    data class Result(val result: CommandResult) : ProcessAction()
    data class AppendLogEntry(val entry: LogEntry<MemorySegment>) : ProcessAction()
    data class ApplyCommand(val command: Command, val currentTerm: Long) : ProcessAction()
    data class ApplyCommandLogEntry(val entry: LogEntry<MemorySegment>, val committed: Boolean) : ProcessAction()
    data class WritePersistentState(val state: PersistentState) : ProcessAction()
    data class StartTimeout(val timeout: Timeout) : ProcessAction()
}

interface ActionSink {
    operator fun plusAssign(action: ProcessAction)
    fun removeActionIf(predicate: (ProcessAction) -> Boolean)
}

@RunWith(Parameterized::class)
class MockTestKt(
    private val raftActorId: Int,
    private val nraftActores: Int,
    startTerm: Long,
    lastLogIndex: Long,
) : ActionSink, Environment<MemorySegment> {
    companion object {
        @JvmStatic
        @Parameterized.Parameters(name = "pid{0}/{1} term={2} log={3}")
        fun parameters() =
            listOf(
                arrayOf(1, 3, 0L, 0L),
                arrayOf(2, 3, 5L, 2L),
                arrayOf(3, 5, 7L, 9L),
                arrayOf(4, 7, 11L, 15L)
            )

        private val START_LOG_ID = LogId(0, 0)
    }

    private val rnd = Random(1)
    private val actions = ArrayList<ProcessAction>()

    // private val storage = MockStorage(this, this, rnd, PersistentState(startTerm), lastLogIndex)

    private val dao = BaseDao()

    private val machine = MockDaoStateMachine(this, dao)
    override fun stateMachine(): StateMachine<MemorySegment> = machine

    private var internalGrpcActor = InternalGrpcActorMock(this, nraftActores)

    private val nodeConfig: NodeConfig = Mockito.mock(NodeConfig::class.java).apply {
        Mockito.`when`(this.nodeId).thenReturn(raftActorId)
        Mockito.`when`(this.cluster).thenReturn(List(nraftActores) { "http://localhost:808$it" })
    }

    override fun config(): NodeConfig = nodeConfig

    private var persistentState = PersistentState(startTerm)
    private val replicatedLogManager =
        MockReplicatedLogManager(this, nodeConfig, persistentState, dao, lastLogIndex, rnd)

    override fun replicatedLogManager(): ReplicatedLogManager<MemorySegment> = replicatedLogManager
    private val term: Long get() = replicatedLogManager.readPersistentState().currentTerm
    private val lastLogId: LogId get() = replicatedLogManager.readLastLogId()

    private var externalGrpcActorInterface = Mockito.mock(ExternalGrpcActorInterface::class.java)

    override operator fun plusAssign(action: ProcessAction) {
        actions += action
    }

    override fun removeActionIf(predicate: (ProcessAction) -> Boolean) {
        actions.removeIf(predicate)
    }

    fun send(destId: Int, message: Message) {
        actions += Send(destId, message)
    }

    fun startTimeout(timeout: Timeout) {
        removeActionIf { it is StartTimeout }
        actions += StartTimeout(timeout)
    }

    fun onClientCommandResult(result: CommandResult) {
        actions += Result(result)
    }

    private val raftActor by lazy {
        Mockito.spy(RaftActor(internalGrpcActor, this, externalGrpcActorInterface)).apply {
            Mockito.`when`(this.startTimeout(Mockito.any())).thenAnswer { }
        }
    }

    @Before
    fun initFollower() {
        expectActions(StartTimeout(Timeout.ELECTION_TIMEOUT))
    }

    @After
    fun checkNoMoreActions() {
        expectActions()
    }

    @Test
    fun `FOLLOWER responds on ping in the current term`() {
        val leaderId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        raftActor.onAppendEntry(AppendEntryRpc(leaderId, term, lastLogId, 0, null)) {
            Assertions.assertEquals(term, it.term)
            Assertions.assertEquals(lastLogId.index, it.lastIndex)
        }
        expectActions(
            Send(leaderId, AppendEntryResult(term, lastLogId.index)),
            StartTimeout(Timeout.ELECTION_TIMEOUT)
        )
    }

    @Test
    fun `FOLLOWER responds on ping in the new term`() {
        val leaderId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        val newTerm = term + rnd.nextInt(1..3)
        raftActor.onAppendEntry(AppendEntryRpc(leaderId, newTerm, lastLogId, 0, null))
        expectActions(
            Send(leaderId, AppendEntryResult(newTerm, lastLogId.index)),
            WritePersistentState(PersistentState(newTerm)),
            StartTimeout(Timeout.ELECTION_TIMEOUT)
        )
    }

    @Test
    fun `FOLLOWER votes for a candidate with the same lastLogId`() {
        val leaderId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        val newTerm = term + rnd.nextInt(1..3)
        // Updates term and votes once
        raftActor.onRequestVote(RequestVoteRpc(leaderId, newTerm, lastLogId))
        expectActions(
            Send(leaderId, RequestVoteResult(newTerm, true)),
            WritePersistentState(PersistentState(newTerm, leaderId)),
            StartTimeout(Timeout.ELECTION_TIMEOUT)
        )
        // Votes again for the same candidate
        raftActor.onRequestVote(RequestVoteRpc(leaderId, newTerm, lastLogId)) {}
        expectActions(
            Send(leaderId, RequestVoteResult(newTerm, true)),
        )
    }

    @Test
    fun `FOLLOWER votes for a candidate with more up-to-date log (new term, old index)`() {
        if (term <= 1 || lastLogId.index == 0L) return
        val oldIndex = findOldLogId().index
        val candidateId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        val newTerm = term + rnd.nextInt(1..3)
        val oldLogId = LogId(oldIndex, newTerm)
        raftActor.onRequestVote(RequestVoteRpc(candidateId, newTerm, oldLogId))
        expectActions(
            WritePersistentState(PersistentState(newTerm, candidateId)),
            Send(candidateId, RequestVoteResult(newTerm, true)),
            StartTimeout(Timeout.ELECTION_TIMEOUT)
        )
    }

    @Test
    fun `FOLLOWER refuses to vote for a different candidate`() {
        val candidateId1 = raftActorId % nraftActores + 1
        val candidateId2 = candidateId1 % nraftActores + 1
        val newTerm = term + rnd.nextInt(1..3)
        // Updates term and grants vote to leaderId1
        raftActor.onRequestVote(RequestVoteRpc(candidateId1, newTerm, lastLogId))
        expectActions(
            Send(candidateId1, RequestVoteResult(newTerm, true)),
            WritePersistentState(PersistentState(newTerm, candidateId1)),
            StartTimeout(Timeout.ELECTION_TIMEOUT)
        )
        // Refuses vote to leaderId2
        raftActor.onRequestVote(RequestVoteRpc(candidateId2, newTerm, lastLogId))
        expectActions(
            Send(candidateId2, RequestVoteResult(newTerm, false)),
        )
    }

    @Test
    fun `FOLLOWER refuses to vote for a candidate with not up-to-date log (old index)`() {
        if (lastLogId.index <= 0) return
        val candidateId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        val newTerm = term + rnd.nextInt(1..3)
        val oldLogId = replicatedLogManager.readLog(rnd.nextLong(0L until lastLogId.index))?.logId()
            ?: START_LOG_ID
        raftActor.onRequestVote(RequestVoteRpc(candidateId, newTerm, oldLogId))
        expectActions(
            WritePersistentState(PersistentState(newTerm)),
            Send(candidateId, RequestVoteResult(newTerm, false)),
            StartTimeout(Timeout.ELECTION_TIMEOUT)
        )
    }

    @Test
    fun `FOLLOWER refuses to vote for a candidate with not up-to-date log (old term)`() {
        if (lastLogId.term <= 1) return
        val candidateId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        val newTerm = term + rnd.nextInt(1..3)
        val oldLogId = LogId(lastLogId.index, lastLogId.term - 1)
        raftActor.onRequestVote(RequestVoteRpc(candidateId, newTerm, oldLogId))
        expectActions(
            WritePersistentState(PersistentState(newTerm)),
            Send(candidateId, RequestVoteResult(newTerm, false)),
            StartTimeout(Timeout.ELECTION_TIMEOUT)
        )
    }

    @Test
    fun `FOLLOWER refuses to vote for a candidate with not up-to-date log (new index, old term)`() {
        if (lastLogId.term <= 1) return
        val candidateId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        val newTerm = term + rnd.nextInt(1..3)
        val newIndex = lastLogId.index + rnd.nextInt(1..3)
        val oldLogId = LogId(newIndex, lastLogId.term - 1)
        raftActor.onRequestVote(RequestVoteRpc(candidateId, newTerm, oldLogId))
        expectActions(
            WritePersistentState(PersistentState(newTerm)),
            Send(candidateId, RequestVoteResult(newTerm, false)),
            StartTimeout(Timeout.ELECTION_TIMEOUT)
        )
    }

    @Test
    fun `FOLLOWER refuses to vote for a stale candidate`() {
        if (term == 0L) return
        val candidateId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        val oldTerm = rnd.nextLong(0L until term)
        raftActor.onRequestVote(RequestVoteRpc(candidateId, oldTerm, lastLogId))
        expectActions(
            Send(candidateId, RequestVoteResult(term, false))
        )
    }

    @Test
    fun `FOLLOWER appends matching log entries`() {
        val leaderId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        val newTerm = term + rnd.nextInt(1..3)
        // Append one entry
        val entry1 = rnd.nextLogEntry(lastLogId.index + 1, newTerm)
        raftActor.onAppendEntry(AppendEntryRpc(leaderId, newTerm, lastLogId, 0, entry1))
        expectActions(
            Send(leaderId, AppendEntryResult(newTerm, entry1.logId.index)),
            WritePersistentState(PersistentState(newTerm)),
            AppendLogEntry(entry1),
            StartTimeout(Timeout.ELECTION_TIMEOUT)
        )
        // Append another entry from the same term
        val entry2 = rnd.nextLogEntry(lastLogId.index + 1, newTerm)
        raftActor.onAppendEntry(AppendEntryRpc(leaderId, newTerm, entry1.logId, 0, entry2))
        expectActions(
            Send(leaderId, AppendEntryResult(newTerm, entry2.logId.index)),
            AppendLogEntry(entry2),
            StartTimeout(Timeout.ELECTION_TIMEOUT)
        )
    }

    @Test
    fun `FOLLOWER rejects unmatched log entry`() {
        val leaderId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        val newTerm = term + rnd.nextInt(1..3)
        val entry = rnd.nextLogEntry(lastLogId.index + 1, newTerm)
        raftActor.onAppendEntry(AppendEntryRpc(leaderId, newTerm, entry.logId, 0, entry))
        expectActions(
            Send(leaderId, AppendEntryResult(newTerm, null)),
            WritePersistentState(PersistentState(newTerm)),
            StartTimeout(Timeout.ELECTION_TIMEOUT)
        )
    }

    @Test
    fun `FOLLOWER commits all log entries on leader commit`() {
        val leaderId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        val lastLogIndex = lastLogId.index
        raftActor.onAppendEntry(AppendEntryRpc(leaderId, term, lastLogId, lastLogIndex, null))
        expectActions(
            (1L..lastLogIndex).map { ApplyCommandLogEntry(replicatedLogManager.readLog(it)!!, true) },
            Send(leaderId, AppendEntryResult(term, lastLogIndex)),
            StartTimeout(Timeout.ELECTION_TIMEOUT)
        )
    }

    @Test
    fun `FOLLOWER commits some log entries on leader commit`() {
        val leaderId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        val lastLogIndex = lastLogId.index
        val leaderCommit = rnd.nextLong(0L..lastLogIndex)
        raftActor.onAppendEntry(AppendEntryRpc(leaderId, term, lastLogId, leaderCommit, null))
        expectActions(
            (1..leaderCommit).map { ApplyCommandLogEntry(replicatedLogManager.readLog(it)!!, true) },
            Send(leaderId, AppendEntryResult(term, lastLogIndex)),
            StartTimeout(Timeout.ELECTION_TIMEOUT)
        )
    }

    @Test
    fun `FOLLOWER rejects commit message from stale leader`() {
        if (term == 0L) return
        val leaderId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        val oldTerm = rnd.nextLong(0L until term)
        raftActor.onAppendEntry(AppendEntryRpc(leaderId, oldTerm, lastLogId, lastLogId.index, null))
        expectActions(
            Send(leaderId, AppendEntryResult(term, null))
        )
    }

    @Test
    fun `FOLLOWER queues direct client command and forwards it to leader on ping`() {
        val leaderId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        // the first command
        val command1 = rnd.nextCommand(raftActorId.toLong())
        raftActor.onClientCommand(command1)
        expectActions() // nothing
        raftActor.onAppendEntry(AppendEntryRpc(leaderId, term, lastLogId, 0, null))
        expectActions(
            Send(leaderId, AppendEntryResult(term, lastLogId.index)),
            Send(leaderId, ClientCommandRpc(raftActorId, command1)),
            StartTimeout(Timeout.ELECTION_TIMEOUT)
        )
        // one more command
        val command2 = rnd.nextCommand(raftActorId.toLong())
        raftActor.onClientCommand(command2)
        expectActions(
            Send(leaderId, ClientCommandRpc(raftActorId, command2))
        )
    }

    @Test
    fun `FOLLOWER queues forwarded client commands and forwards it to leader on ping`() {
        if (term <= 1) return
        val oldTerm = rnd.nextInt(1..<term.toInt())
        val clientId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        val leaderId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        // the first command
        val command1 = rnd.nextCommand(clientId.toLong())
        raftActor.onClientCommand(command1)
        expectActions() // nothing
        raftActor.onAppendEntry(AppendEntryRpc(leaderId, term, lastLogId, 0, null))
        expectActions(
            Send(leaderId, AppendEntryResult(term, lastLogId.index)),
            Send(leaderId, ClientCommandRpc(raftActorId, command1)),
            StartTimeout(Timeout.ELECTION_TIMEOUT)
        )
        // one more stable command from old term
        val command2 = rnd.nextCommand(clientId)
        raftActor.onClientCommand(command2)
        expectActions(
            Send(leaderId, ClientCommandRpc(raftActorId, command2))
        )
    }

    @Test
    fun `FOLLOWER reports command result`() {
        val leaderId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        val result = rnd.nextCommandResult(term)
        raftActor.onClientCommandResult(null, result)
        expectActions(
            Result(result)
        )
    }

    @Test
    fun `FOLLOWER learns new term leader on command result and forwards its queue`() {
        // command to the follow who does not know the leader yet
        val myCommand = rnd.nextCommand(raftActorId)
        raftActor.onClientCommand(myCommand)
        // message from some new leader
        val leaderId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        val newTerm = term + rnd.nextInt(1..3)
        val result = rnd.nextCommandResult(newTerm)
        raftActor.onClientCommandResult(null, result)
        expectActions(
            Result(result),
            WritePersistentState(PersistentState(newTerm)),
            StartTimeout(Timeout.ELECTION_TIMEOUT), // must restart waiting heartbeats from the new leader
            Send(leaderId, ClientCommandRpc(raftActorId, myCommand)) // forwards its client command to the new leader
        )
    }

    @Test
    fun `FOLLOWER replies 'no success' to AppendEntryRpc from an old term`() {
        if (term <= 1 || lastLogId.index == 0L) return
        val oldLogId = findOldLogId()
        val oldTerm = oldLogId.term
        val oldIndex = oldLogId.index
        val oldLeaderId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        val prevLogId = replicatedLogManager.readLog(oldIndex - 1)?.logId() ?: START_LOG_ID
        val oldEntry = rnd.nextLogEntry(oldIndex, oldTerm)
        raftActor.onAppendEntry(AppendEntryRpc(oldLeaderId, oldTerm, prevLogId, oldIndex, oldEntry))
        expectActions(
            Send(oldLeaderId, AppendEntryResult(term, null))
        )
    }

    @Test
    fun `FOLLOWER ignores AppendEntryResult from an old term`() {
        if (term <= 1) return
        val oldTerm = rnd.nextLong(1 until term)
        val followerId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        raftActor.onAppendEntryResult(followerId, AppendEntryResult(oldTerm, null))
        expectActions()
        raftActor.onAppendEntryResult(followerId, AppendEntryResult(oldTerm, 1))
        expectActions()
    }

    @Test
    fun `FOLLOWER ignores RequestVoteResult from an old term`() {
        if (term <= 1 || lastLogId.index == 0L) return
        val oldLogId = findOldLogId()
        val oldTerm = oldLogId.term
        val candidateId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        raftActor.onRequestVoteResult(candidateId, RequestVoteResult(oldTerm, true))
        expectActions()
    }

    private fun initCandidate() {
        val nextTerm = term + 1
        raftActor.onTimeout()
        expectCandidateVoteRequest(nextTerm)
    }

    private fun expectCandidateVoteRequest(term: Long) {
        expectActions(
            (1..nraftActores).filter { it != raftActorId }.map {
                Send(it, RequestVoteRpc(raftActorId, term, lastLogId))
            },
            WritePersistentState(PersistentState(term, raftActorId)),
            StartTimeout(Timeout.ELECTION_TIMEOUT)
        )
    }

    @Test
    fun `CANDIDATE requests votes on timeouts`() {
        initCandidate()
        initCandidate()
    }

    @Test
    fun `CANDIDATE does not become a leader without enough votes`() {
        initCandidate()
        val otherIds = (1..nraftActores).filter { it != raftActorId }
        val grantedVote = otherIds.shuffled(rnd).take(nraftActores / 2 - 1).toSet()
        for (id in otherIds.shuffled(rnd)) {
            raftActor.onRequestVoteResult(id, RequestVoteResult(term, id in grantedVote))
            expectActions()
        }
    }

    @Test
    fun `CANDIDATE refuses to vote for another one in the same term`() {
        initCandidate()
        val leaderId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        raftActor.onRequestVote(RequestVoteRpc(leaderId, term, lastLogId))
        expectActions(
            Send(leaderId, RequestVoteResult(term, false))
        )
    }

    @Test
    fun `CANDIDATE refuses to vote for a stale candidate`() {
        if (term == 0L) return
        val candidateId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        val oldTerm = rnd.nextLong(0L until term)
        raftActor.onRequestVote(RequestVoteRpc(candidateId, oldTerm, lastLogId))
        expectActions(
            Send(candidateId, RequestVoteResult(term, false))
        )
    }

    @Test
    fun `CANDIDATE queues client commands and forwards them to leader on ping`() {
        // the first command
        val command1 = rnd.nextCommand(raftActorId)
        raftActor.onClientCommand(command1)
        expectActions() // nothing
        // becomes candidate
        initCandidate()
        // the second command
        val command2 = rnd.nextCommand(raftActorId)
        raftActor.onClientCommand(command2)
        expectActions() // nothing
        // ping from the leader
        val leaderId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        raftActor.onAppendEntry(AppendEntryRpc(leaderId, term, lastLogId, 0, null))
        expectActions(
            Send(leaderId, AppendEntryResult(term, lastLogId.index)),
            Send(leaderId, ClientCommandRpc(raftActorId, command1)),
            Send(leaderId, ClientCommandRpc(raftActorId, command2)),
            StartTimeout(Timeout.ELECTION_TIMEOUT)
        )
        // one more command
        val command3 = rnd.nextCommand(raftActorId)
        raftActor.onClientCommand(command3)
        expectActions(
            Send(leaderId, ClientCommandRpc(raftActorId, command3))
        )
    }

    @Test
    fun `CANDIDATE reports client command result from old term when it was a follower`() {
        if (term <= 1) return
        val oldTerm = term - 1
        val oldLeaderId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        initCandidate()
        val result = rnd.nextCommandResult(oldTerm)
        raftActor.onClientCommandResult(null, result)
        expectActions(
            Result(result)
        )
    }

    @Test
    fun `CANDIDATE replies 'no success' to AppendEntryRpc from an old term`() {
        if (term <= 1 || lastLogId.index == 0L) return
        val oldLogId = findOldLogId()
        val oldTerm = oldLogId.term
        val oldIndex = oldLogId.index
        val oldLeaderId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        val prevLogId = replicatedLogManager.readLog(oldIndex - 1)?.logId() ?: START_LOG_ID
        val oldEntry = rnd.nextLogEntry(oldIndex, oldTerm)
        initCandidate()
        raftActor.onAppendEntry(AppendEntryRpc(oldLeaderId, oldTerm, prevLogId, oldIndex, oldEntry))
        expectActions(
            Send(oldLeaderId, AppendEntryResult(term, null))
        )
    }

    @Test
    fun `CANDIDATE ignores AppendEntryResult from an old term`() {
        if (term <= 1) return
        val oldTerm = rnd.nextLong(1L until term)
        val followerId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        initCandidate()
        raftActor.onAppendEntryResult(followerId, AppendEntryResult(oldTerm, null))
        expectActions()
        raftActor.onAppendEntryResult(followerId, AppendEntryResult(oldTerm, 1))
        expectActions()
    }

    @Test
    fun `CANDIDATE gathers enough votes and becomes leader`() {
        initLeader()
    }

    private fun initLeader() {
        initCandidate()
        val ids = (1..nraftActores).filter { it != raftActorId }.shuffled(rnd).take(nraftActores / 2)
        for (id in ids) {
            expectActions() // while not last
            raftActor.onRequestVoteResult(id, RequestVoteResult(term, true))
        }
        // became leader, sends heartbeats
        expectHeartbeats()
    }

    private fun expectHeartbeats() {
        expectActions(
            (1..nraftActores).filter { it != raftActorId }.map {
                Send(it, AppendEntryRpc(raftActorId, term, lastLogId, 0, null))
            },
            StartTimeout(Timeout.LEADER_HEARTBEAT_PERIOD)
        )
    }

    @Test
    fun `LEADER sends heartbeats on timeout`() {
        initLeader()
        raftActor.onTimeout()
        expectHeartbeats()
    }

    @Test
    fun `LEADER receivers ping and becomes follower`() {
        initLeader()
        val leaderId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        val newTerm = term + rnd.nextInt(1..3)
        raftActor.onAppendEntry(AppendEntryRpc(leaderId, newTerm, lastLogId, 0, null))
        expectActions(
            Send(leaderId, AppendEntryResult(newTerm, lastLogId.index)),
            WritePersistentState(PersistentState(newTerm)),
            StartTimeout(Timeout.ELECTION_TIMEOUT)
        )
    }

    @Test
    fun `LEADER receivers entry and becomes follower`() {
        initLeader()
        val leaderId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        val newTerm = term + rnd.nextInt(1..3)
        // Append one entry
        val entry1 = rnd.nextLogEntry(lastLogId.index + 1, newTerm)
        raftActor.onAppendEntry(AppendEntryRpc(leaderId, newTerm, lastLogId, 0, entry1))
        expectActions(
            Send(leaderId, AppendEntryResult(newTerm, entry1.logId.index)),
            WritePersistentState(PersistentState(newTerm)),
            AppendLogEntry(entry1),
            StartTimeout(Timeout.ELECTION_TIMEOUT)
        )
        // Append another entry from the same term
        val entry2 = rnd.nextLogEntry(lastLogId.index + 1, newTerm)
        raftActor.onAppendEntry(AppendEntryRpc(leaderId, newTerm, entry1.logId, 0, entry2))
        expectActions(
            Send(leaderId, AppendEntryResult(newTerm, entry2.logId.index)),
            AppendLogEntry(entry2),
            StartTimeout(Timeout.ELECTION_TIMEOUT)
        )
    }

    @Test
    fun `LEADER applies direct client commands`() {
        initLeader()
        // the first command
        val command1 = rnd.nextCommand(raftActorId)
        val lastLogId1 = lastLogId
        val entry1 = command1.toLogEntry(LogId(lastLogId1.index + 1, term))
        raftActor.onClientCommand(command1)
        expectActions(
            (1..nraftActores).filter { it != raftActorId }.map {
                Send(it, AppendEntryRpc(raftActorId, term, lastLogId1, 0, entry1))
            },
            AppendLogEntry(entry1)
        )
        // subsequent command will not generate AppendEntryRpc (until they've responded to)
        repeat(2) {
            val command2 = rnd.nextCommand(raftActorId)
            val lastLogId2 = lastLogId
            val entry2 = command2.toLogEntry(LogId(lastLogId2.index + 1, term))
            raftActor.onClientCommand(command2)
            expectActions(
                AppendLogEntry(entry2)
            )
        }
    }

    @Test
    fun `LEADER applies forwarded client commands`() {
        initLeader()
        // the first command
        val clientId1 = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        val command1 = rnd.nextCommand(clientId1)
        val lastLogId1 = lastLogId
        val entry1 = command1.toLogEntry(LogId(lastLogId1.index + 1, term))
        raftActor.onClientCommand(command1)
        expectActions(
            (1..nraftActores).filter { it != raftActorId }.map {
                Send(it, AppendEntryRpc(raftActorId, term, lastLogId1, 0, entry1))
            },
            AppendLogEntry(entry1)
        )
        // subsequent command will not generate AppendEntryRpc (until they've responded to)
        repeat(2) {
            val clientId2 = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
            val command2 = rnd.nextCommand(clientId2)
            val lastLogId2 = lastLogId
            val entry2 = command2.toLogEntry(LogId(lastLogId2.index + 1, term))
            raftActor.onClientCommand(command2)
            expectActions(
                AppendLogEntry(entry2)
            )
        }
    }

    @Test
    fun `LEADER scans back log for on mismatched follower`() {
        initLeader()
        val followerId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        var lasIndex = lastLogId.index
        repeat(3) {
            if (lasIndex == 0L) return
            raftActor.onAppendEntryResult(followerId, AppendEntryResult(term, null))
            expectActions(
                Send(
                    followerId, AppendEntryRpc(
                        raftActorId,
                        term,
                        prevLogId = replicatedLogManager.readLog(lasIndex - 1)?.logId() ?: START_LOG_ID,
                        leaderCommit = 0,
                        entry = replicatedLogManager.readLog(lasIndex)
                    )
                )
            )
            lasIndex--
        }
    }

    private fun initLeaderWithNextIndices() {
        initLeader()
        // Ok response on leader's pings from all raftActores
        for (id in 1..nraftActores) if (id != raftActorId) {
            raftActor.onAppendEntryResult(id, AppendEntryResult(term, lastLogId.index))
            expectActions()
        }
    }

    @Test
    fun `LEADER commits direct client commands`() {
        initLeaderWithNextIndices()
        // raftActor commands
        var leaderCommit = 0L
        val expectedMachine = DaoStateMachine(BaseDao())
        for (i in 1..lastLogId.index) expectedMachine.apply(replicatedLogManager.readLog(i)!!, true)
        repeat(3) {
            val command = rnd.nextCommand(raftActorId)
            val lastLogId = lastLogId
            val entry = command.toLogEntry(LogId(lastLogId.index + 1, term))
            raftActor.onClientCommand(command)
            expectActions(
                (1..nraftActores).filter { it != raftActorId }.map {
                    Send(it, AppendEntryRpc(raftActorId, term, lastLogId, leaderCommit, entry))
                },
                AppendLogEntry(entry)
            )
            // responses in random order
            val ids = (1..nraftActores).filter { it != raftActorId }.shuffled(rnd)
            var count = 0
            for (id in ids) {
                raftActor.onAppendEntryResult(id, AppendEntryResult(term, entry.logId().index))
                if (++count == nraftActores / 2) { // commit on majority of answers
                    val result = expectedMachine.apply(command, term) // FIXME
                    expectActions(
                        (leaderCommit + 1..entry.logId().index).map {
                            ApplyCommandLogEntry(replicatedLogManager.readLog(it)!!, true)
                        },
                        Result(result)
                    )
                } else {
                    expectActions() // nothin special otherwise
                }
            }
            leaderCommit = entry.logId().index
        }
    }

    @Test
    fun `LEADER commits forwarded client commands`() {
        initLeaderWithNextIndices()
        // raftActor commands
        var leaderCommit = 0L
        val expectedMachine = DaoStateMachine(BaseDao())
        for (i in 1..lastLogId.index) expectedMachine.apply(replicatedLogManager.readLog(i)!!, true)
        repeat(3) {
            val clientId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
            val command = rnd.nextCommand(clientId)
            val lastLogId = lastLogId
            val entry = command.toLogEntry(LogId(lastLogId.index + 1, term))
            raftActor.onClientCommand(command)
            expectActions(
                (1..nraftActores).filter { it != raftActorId }.map {
                    Send(it, AppendEntryRpc(raftActorId, term, lastLogId, leaderCommit, entry))
                },
                AppendLogEntry(entry)
            )
            // responses in random order
            val ids = (1..nraftActores).filter { it != raftActorId }.shuffled(rnd)
            var count = 0
            for (id in ids) {
                raftActor.onAppendEntryResult(id, AppendEntryResult(term, entry.logId().index))
                if (++count == nraftActores / 2) { // commit on majority of answers
                    val result = expectedMachine.apply(command, term) // FIXME
                    expectActions(
                        (leaderCommit + 1..entry.logId().index).map {
                            ApplyCommandLogEntry(replicatedLogManager.readLog(it)!!, true)
                        },
                        Send(clientId, ClientCommandResult(term, result))
                    )
                } else {
                    expectActions() // nothing special otherwise
                }
            }
            leaderCommit = entry.logId().index
        }
    }

    @Test
    fun `LEADER reports client command result from old term when it was a follower`() {
        if (term <= 1) return
        val oldTerm = term - 1
        initLeader()
        val result = rnd.nextCommandResult(oldTerm)
        raftActor.onClientCommandResult(null, result)
        expectActions(
            Result(result)
        )
    }

    @Test
    fun `LEADER replies 'no success' to AppendEntryRpc from an old term`() {
        if (term <= 1 || lastLogId.index == 0L) return
        val oldLogId = findOldLogId()
        val oldTerm = oldLogId.term
        val oldIndex = oldLogId.index
        val oldLeaderId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        val prevLogId = replicatedLogManager.readLog(oldIndex - 1)?.logId() ?: START_LOG_ID
        val oldEntry = rnd.nextLogEntry(oldIndex, oldTerm)
        initLeader()
        raftActor.onAppendEntry(AppendEntryRpc(oldLeaderId, oldTerm, prevLogId, oldIndex, oldEntry))
        expectActions(
            Send(oldLeaderId, AppendEntryResult(term, null))
        )
    }

    private fun findOldLogId(): LogId {
        while (true) {
            val id = replicatedLogManager.readLog(rnd.nextLong(1..lastLogId.index))!!.logId()
            if (id.term < term) return id
        }
    }

    @Test
    fun `LEADER refuses to vote for a stale candidate`() {
        if (term == 0L) return
        val candidateId = (raftActorId + rnd.nextInt(nraftActores - 1)) % nraftActores + 1
        val oldTerm = rnd.nextLong(0 until term)
        initLeader()
        raftActor.onRequestVote(RequestVoteRpc(candidateId, oldTerm, lastLogId))
        expectActions(
            Send(candidateId, RequestVoteResult(term, false))
        )
    }

    private fun expectActions(expected: List<ProcessAction>, vararg more: ProcessAction) =
        expectActions(*expected.toTypedArray(), *more)

    private fun expectActions(vararg expected: ProcessAction) {
        for (action in expected) {
            assert(actions.remove(action)) {
                "Expected action: $action, but was only:\n${actions.joinToString("\n") { "\t\t$it" }}"
            }
        }
        assert(actions.isEmpty()) {
            "Expected no other actions:\n${actions.joinToString("\n") { "\t\t$it" }}"
        }
        actions.clear()
    }

    @Suppress("TestFunctionName")
    private fun AppendEntryRpc(
        leaderId: Int,
        term: Long,
        prevLogId: LogId,
        leaderCommit: Long,
        entry: LogEntry<MemorySegment>?
    ): AppendEntriesRequest {
        val builder = AppendEntriesRequest.newBuilder()
            .setTerm(term)
            .setPrevLogTerm(prevLogId.term)
            .setPrevLogIndex(prevLogId.index)
            .setLeaderCommit(leaderCommit)
            .setLeaderId(leaderId)
        if (entry != null) {
            builder.addEntries(EntryConverter.logEntryToRaftLogEntry(entry))
        }
        return builder.build()
    }

    @Suppress("TestFunctionName")
    private fun AppendEntryResult(
        term: Long,
        lastIndex: Long?,
    ): Raft.AppendEntriesResponse {
        return Raft.AppendEntriesResponse.newBuilder()
            .setTerm(term)
            .setLastIndex(lastIndex ?: -1)
            .build()
    }

    @Suppress("TestFunctionName")
    private fun RequestVoteRpc(candidateId: Int, term: Long, lastLogId: LogId): VoteRequest {
        return VoteRequest.newBuilder()
            .setTerm(term)
            .setLastLogTerm(lastLogId.term)
            .setLastLogIndex(lastLogId.index)
            .setCandidateId(candidateId)
            .build()
    }

    @Suppress("TestFunctionName")
    private fun RequestVoteResult(term: Long, voteGranted: Boolean): Raft.VoteResponse {
        return Raft.VoteResponse.newBuilder()
            .setTerm(term)
            .setVoteGranted(voteGranted)
            .build()
    }

    @Suppress("TestFunctionName")
    private fun ClientCommandRpc(pid: Int, command: Command): Raft.ClientCommandRequestRPC {
        val builder = Raft.ClientCommandRequestRPC.newBuilder()
            .setKey(ByteString.copyFromUtf8(command.key()))
            .setProcessId(pid)
        if (command is InsertCommand) {
            if (command.value() != null) {
                builder
                    .setValue(ByteString.copyFromUtf8(command.value()))
                    .setOperation(Raft.Operation.PUT)
            } else {
                builder
                    .setOperation(Raft.Operation.DELETE)
            }
            builder.setUncommitted(false)
        } else {
            builder
                .setOperation(Raft.Operation.GET)
        }
        return builder.build()
    }

    /** Result of the client command for the original [Command.processId]. Sent by the leader. */
    @Suppress("TestFunctionName")
    private fun ClientCommandResult(term: Long, result: CommandResult): Raft.ClientCommandResponseRPC {
        return Raft.ClientCommandResponseRPC.newBuilder()
            .setTerm(term)
            .setKey(ByteString.copyFromUtf8(result.key()))
            .setValue(ByteString.copyFromUtf8(result.value()))
            .build()
    }

    private fun RaftActor.onAppendEntry(appendEntriesRequest: AppendEntriesRequest?) =
        onAppendEntry(appendEntriesRequest) {}

    private fun RaftActor.onRequestVote(voteRequest: VoteRequest?) = onRequestVote(voteRequest) {}
}
