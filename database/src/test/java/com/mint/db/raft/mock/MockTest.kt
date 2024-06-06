package com.mint.db.raft.mock

import com.google.protobuf.Message
import com.mint.db.config.NodeConfig
import com.mint.db.dao.impl.BaseDao
import com.mint.db.grpc.ExternalGrpcActorInterface
import com.mint.db.grpc.InternalGrpcActor
import com.mint.db.raft.Environment
import com.mint.db.raft.RaftActor
import com.mint.db.raft.StateMachine
import com.mint.db.raft.Timeout
import com.mint.db.raft.mock.ProcessAction.Result
import com.mint.db.raft.mock.ProcessAction.Send
import com.mint.db.raft.mock.ProcessAction.StartTimeout
import com.mint.db.raft.model.Command
import com.mint.db.raft.model.CommandResult
import com.mint.db.replication.ReplicatedLogManager
import com.mint.db.replication.model.LogEntry
import com.mint.db.replication.model.PersistentState
import org.junit.After
import org.junit.Before
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.mockito.Mockito
import java.lang.foreign.MemorySegment
import kotlin.random.Random

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
    private val processId: Int,
    private val nProcesses: Int,
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
    }

    private val rnd = Random(1)
    private val actions = ArrayList<ProcessAction>()

    // private val storage = MockStorage(this, this, rnd, PersistentState(startTerm), lastLogIndex)

    private val dao = BaseDao()

    private val machine = MockDaoStateMachine(this, dao)
    override fun stateMachine(): StateMachine<MemorySegment> = machine

    private var internalGrpcActor = Mockito.mock(InternalGrpcActor::class.java)

    private val nodeConfig: NodeConfig = Mockito.mock(NodeConfig::class.java).apply {
        Mockito.`when`(this.nodeId).thenReturn(processId)
        Mockito.`when`(this.cluster).thenReturn(List(nProcesses) { "http://localhost:808$it" })
    }

    override fun config(): NodeConfig = nodeConfig

    private var persistentState = PersistentState(startTerm)
    private val replicatedLogManager =
        MockReplicatedLogManager(this, nodeConfig, persistentState, dao, lastLogIndex, rnd)

    override fun replicatedLogManager(): ReplicatedLogManager<MemorySegment> = replicatedLogManager

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
}
