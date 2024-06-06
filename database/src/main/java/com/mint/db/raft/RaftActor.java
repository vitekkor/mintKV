package com.mint.db.raft;

import com.mint.db.Raft;
import com.mint.db.config.NodeConfig;
import com.mint.db.dao.Entry;
import com.mint.db.dao.impl.BaseEntry;
import com.mint.db.dao.impl.StringDaoWrapper;
import com.mint.db.grpc.ExternalGrpcActorInterface;
import com.mint.db.grpc.InternalGrpcActorInterface;
import com.mint.db.raft.model.*;
import com.mint.db.replication.ReplicatedLogManager;
import com.mint.db.replication.impl.ReplicatedLogManagerImpl;
import com.mint.db.replication.model.LogEntry;
import com.mint.db.replication.model.PersistentState;
import com.mint.db.replication.model.impl.BaseLogEntry;
import com.mint.db.replication.model.impl.OperationType;
import com.mint.db.util.EntryConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.lang.foreign.MemorySegment;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.mint.db.util.LogUtil.protobufMessageToString;

public class RaftActor implements RaftActorInterface {
    public static final String MDC_NODE_ID = "nodeId";
    private static final Logger logger = LoggerFactory.getLogger(RaftActor.class);
    private static final Random rand = new Random();
    private static final int POOL_SIZE = 1;
    private final InternalGrpcActorInterface internalGrpcActor;
    private final int nodeId;
    private final ScheduledExecutorService scheduledExecutor;
    private final ReplicatedLogManager<MemorySegment> replicatedLogManager;
    private final StringDaoWrapper dao;
    private final NodeConfig config;
    private final Queue<Command> queue = new ArrayDeque<>();
    private final ExternalGrpcActorInterface externalGrpcActorInterface;
    private ScheduledFuture<?> scheduledFuture;
    private int votedForMe = 0;
    private int leaderId = -1;
    private long nextTimeout = Long.MAX_VALUE;
    private final long[] nextIndex;
    private long lastApplied;
    private final long[] matchIndex;

    private StateMachine<MemorySegment> stateMachine;

    public RaftActor(
            InternalGrpcActorInterface internalGrpcActor,
            NodeConfig config,
            PersistentState state,
            ExternalGrpcActorInterface externalGrpcActorInterface
    ) {
        this.externalGrpcActorInterface = externalGrpcActorInterface;
        this.scheduledExecutor = Executors.newScheduledThreadPool(POOL_SIZE);
        this.dao = new StringDaoWrapper();
        this.replicatedLogManager = new ReplicatedLogManagerImpl(config, state, dao);
        this.config = config;
        this.internalGrpcActor = internalGrpcActor;
        this.nodeId = config.getNodeId();
        this.lastApplied = replicatedLogManager.readLastLogId().index();
        this.nextIndex = new long[config.getCluster().size()];
        for (int i = 0; i < config.getCluster().size(); i++) {
            nextIndex[i] = replicatedLogManager.readLastLogId().index() + 1;
        }
        this.matchIndex = new long[config.getCluster().size()];

        startTimeout(Timeout.ELECTION_TIMEOUT);
    }

    private static int quorum(final int clusterSize) {
        return clusterSize / 2 + 1;
    }

    private static int compareIdLogs(long term, long index, LogId logId) {
        if (term != logId.term()) {
            return Long.compare(term, logId.term());
        }
        return Long.compare(index, logId.index());
    }

    private static int compareIdLogs(LogId logId, LogId otherLogId) {
        if (logId.term() != otherLogId.term()) {
            return Long.compare(logId.term(), otherLogId.term());
        }
        return Long.compare(logId.index(), otherLogId.index());
    }

    private void startTimeout(Timeout timeout) {
        long heartbeatTimeoutMs = config.getHeartbeatTimeoutMs();
        int numberOfProcesses = Math.max(config.getCluster().size(), 2);
        this.nextTimeout = heartbeatTimeoutMs + switch (timeout) {
            case ELECTION_TIMEOUT -> config.heartbeatRandom()
                    ? rand.nextLong(heartbeatTimeoutMs / numberOfProcesses, heartbeatTimeoutMs)
                    : nodeId * heartbeatTimeoutMs / numberOfProcesses;
            case LEADER_HEARTBEAT_PERIOD -> 0;
            case null -> throw new IllegalArgumentException();
        };

        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }

        scheduledFuture = scheduledExecutor.schedule(
                this::onTimeout,
                nextTimeout,
                TimeUnit.MILLISECONDS
        );
    }

    @Override
    public void onTimeout() {
        if (leaderId == nodeId) { // send heartbeat
            PersistentState state = replicatedLogManager.readPersistentState();
            for (int i = 0; i < config.getCluster().size(); i++) {
                if (i != nodeId) {
                    // if follower lagging behind from our log
                    // then we send nextIndex entry to him
                    if (nextIndex[i] <= lastApplied) { // todo send batch
                        LogId prevLogId = replicatedLogManager.readLog(nextIndex[i] - 1).logId();
                        LogEntry<MemorySegment> nextEntry = replicatedLogManager.readLog(nextIndex[i]);
                        Raft.AppendEntriesRequest appendEntriesRequest = Raft.AppendEntriesRequest.newBuilder()
                                .setTerm(state.currentTerm())
                                .setLeaderId(nodeId)
                                .setPrevLogIndex(prevLogId.index())
                                .setPrevLogTerm(prevLogId.term())
                                .setEntries(0, EntryConverter.logEntryToRaftLogEntry(nextEntry))
                                .setLeaderCommit(replicatedLogManager.commitIndex())
                                .build();
                        internalGrpcActor.sendAppendEntriesRequest(i, appendEntriesRequest, this::onAppendEntryResult);
                        internalGrpcActor.sendAppendEntriesRequest(appendEntriesRequest, this::onAppendEntryResult);
                    } else {
                        LogId logId = replicatedLogManager.readLastLogId();
                        Raft.AppendEntriesRequest appendEntriesRequest = Raft.AppendEntriesRequest.newBuilder()
                                .setTerm(state.currentTerm())
                                .setLeaderId(nodeId)
                                .setPrevLogIndex(logId.index())
                                .setPrevLogTerm(logId.term())
                                .setLeaderCommit(replicatedLogManager.commitIndex())
                                .build();
                        internalGrpcActor.sendAppendEntriesRequest(i, appendEntriesRequest, this::onAppendEntryResult);
                    }
                }
            }
            startTimeout(Timeout.LEADER_HEARTBEAT_PERIOD);
        } else { // become a follower
            PersistentState oldState = replicatedLogManager.readPersistentState();

            PersistentState state = new PersistentState(oldState.currentTerm() + 1, nodeId);
            replicatedLogManager.writePersistentState(state);
            votedForMe = 0;
            leaderId = -1;

            startTimeout(Timeout.ELECTION_TIMEOUT);

            LogId lastLogId = replicatedLogManager.readLastLogId();
            Raft.VoteRequest voteRequest = Raft.VoteRequest.newBuilder()
                    .setCandidateId(nodeId)
                    .setTerm(state.currentTerm())
                    .setLastLogIndex(lastLogId.index())
                    .setLastLogTerm(lastLogId.term())
                    .build();
            internalGrpcActor.sendVoteRequest(voteRequest, this::onRequestVoteResult);
        }
    }

    @Override
    public void onAppendEntry(
            Raft.AppendEntriesRequest appendEntriesRequest,
            Consumer<Raft.AppendEntriesResponse> onVoteResponse
    ) {
        PersistentState state = replicatedLogManager.readPersistentState();
        LogId prevLogId = replicatedLogManager.readLastLogId();

        if (appendEntriesRequest.getTerm() < state.currentTerm()) { // reject obsolete messages
            Raft.AppendEntriesResponse appendEntriesResponse = Raft.AppendEntriesResponse.newBuilder()
                    .setTerm(state.currentTerm())
                    .setLastIndex(-1)
                    .build();
            onVoteResponse.accept(appendEntriesResponse);
            return;
        }
        LogId appendEntriesRequestPervLogId
                = new LogId(appendEntriesRequest.getPrevLogIndex(), appendEntriesRequest.getPrevLogTerm());

        if (compareIdLogs(prevLogId, appendEntriesRequestPervLogId) < 0) {
            // our log has not up-to-date,
            // so we are sending null to force the leader to send us the missing log
            Raft.AppendEntriesResponse appendEntriesResponse = Raft.AppendEntriesResponse.newBuilder()
                    .setTerm(appendEntriesRequest.getTerm())
                    .setLastIndex(-1)
                    .build();
            onVoteResponse.accept(appendEntriesResponse);
            replicatedLogManager.writePersistentState(new PersistentState(appendEntriesRequest.getTerm()));
            startTimeout(Timeout.ELECTION_TIMEOUT);
            return;
        }

        // if we don't know who leader is, we decide that first node that sends us AppendEntryRPC is the leader
        if (leaderId == -1) {
            leaderId = appendEntriesRequest.getLeaderId();
        }

        if (appendEntriesRequest.getEntriesCount() == 0) {
            onHeartBeat(appendEntriesRequest, state, onVoteResponse);
            return;
        }
        long term = Math.max(appendEntriesRequest.getTerm(), state.currentTerm());
        Raft.LogEntry lastEntry = appendEntriesRequest.getEntries(appendEntriesRequest.getEntriesCount() - 1);
        long index = Math.max(lastEntry.getIndex(), state.currentTerm());

        Raft.AppendEntriesResponse appendEntriesResponse = Raft.AppendEntriesResponse.newBuilder()
                .setTerm(term)
                .setLastIndex(index)
                .build();

        onVoteResponse.accept(appendEntriesResponse);

        if (appendEntriesResponse.getTerm() > state.currentTerm()) {
            // new leader is, our log is up-to-date, so we update our state
            replicatedLogManager.writePersistentState(new PersistentState(appendEntriesRequest.getTerm()));
            leaderId = -1;
        }

        LogId lastEntryLogId = new LogId(lastEntry.getIndex(), lastEntry.getTerm());

        // in the case when the leader thinks that we are lagging behind
        // we shouldn't rewrite our log
        if (compareIdLogs(lastEntryLogId, prevLogId) > 0) {
            appendEntitiesIntoLog(appendEntriesRequest);
            lastApplied = lastEntry.getIndex();
        }
        startTimeout(Timeout.ELECTION_TIMEOUT);

        long lastCommitIndex = replicatedLogManager.commitIndex();

        // commit all entries that were appended by us and committed by leader
        if (appendEntriesRequest.getLeaderCommit() > lastCommitIndex) {
            long newCommitIndex
                    = Math.min(appendEntriesRequest.getLeaderCommit(), replicatedLogManager.readLastLogId().index());
            replicatedLogManager.setCommitIndex(newCommitIndex);
            for (long i = lastCommitIndex + 1; i < newCommitIndex; i++) {
                LogEntry<MemorySegment> logEntry = replicatedLogManager.readLog(i);
                stateMachine.apply(logEntry, true);
            }
        }
    }

    private void onHeartBeat(
            Raft.AppendEntriesRequest appendEntriesRequest,
            PersistentState state,
            Consumer<Raft.AppendEntriesResponse> onVoteResponse
    ) {
        long lastCommitIndex = replicatedLogManager.commitIndex();
        // commit all entries that were appended by us and committed by leader
        if (lastCommitIndex < appendEntriesRequest.getLeaderCommit()) {
            long newCommitIndex = Math.min(lastApplied, appendEntriesRequest.getLeaderCommit());
            for (long i = lastCommitIndex + 1; i < newCommitIndex; i++) {
                LogEntry<MemorySegment> logEntry = replicatedLogManager.readLog(i);
                stateMachine.apply(logEntry, true);
            }
        }

        if (appendEntriesRequest.getTerm() > state.currentTerm()) {
            // new leader is, our log is up-to-date, so we update our state
            PersistentState newState = new PersistentState(appendEntriesRequest.getTerm());
            Raft.AppendEntriesResponse appendEntriesResponse = Raft.AppendEntriesResponse.newBuilder()
                    .setTerm(newState.currentTerm())
                    .setLastIndex(replicatedLogManager.readLastLogId().index())
                    .build();
            onVoteResponse.accept(appendEntriesResponse);
            replicatedLogManager.writePersistentState(newState);
        } else { // just reply
            Raft.AppendEntriesResponse appendEntriesResponse = Raft.AppendEntriesResponse.newBuilder()
                    .setTerm(state.currentTerm())
                    .setLastIndex(replicatedLogManager.readLastLogId().index())
                    .build();
            onVoteResponse.accept(appendEntriesResponse);
        }

        // send all queue commands to the leader
        while (!queue.isEmpty()) {
            onClientCommand(queue.poll());
        }
        startTimeout(Timeout.ELECTION_TIMEOUT);
    }

    @Override
    public void onAppendEntryResult(int srcId, Raft.AppendEntriesResponse appendEntriesResponse) {
        // TODO
        //     1. readPersistentState
        //     2. ignore obsolete messages (appendEntriesResponse.getTerm() < state.currentTerm() -> return)
        //     3. become a follower if  appendEntriesResponse.getTerm() > state.currentTerm() -> update PersistentState
        //       (leaderId = -1, storage.writePersistentState(PersistentState(message.term))
        //            env.startTimeout(Timeout.ELECTION_TIMEOUT))
        //     4.

    }

    public synchronized void onRequestVote(Raft.VoteRequest voteRequest, Consumer<Raft.VoteResponse> onVoteResponse) {
        MDC.put(MDC_NODE_ID, String.valueOf(nodeId));
        logger.info("Receive new VoteRequest {}", protobufMessageToString(voteRequest));

        PersistentState state = replicatedLogManager.readPersistentState();
        // reject old term
        if (voteRequest.getTerm() < state.currentTerm()) {
            Raft.VoteResponse voteResponse = Raft.VoteResponse.newBuilder()
                    .setTerm(state.currentTerm())
                    .setVoteGranted(false)
                    .build();
            onVoteResponse.accept(voteResponse);
            return;
        }

        boolean isAlreadyVotedForAnotherInCurrentTerm =
                state.votedFor() != null
                        && state.votedFor() != voteRequest.getCandidateId()
                        && voteRequest.getTerm() == state.currentTerm();

        // reject new leader in the same term
        if (isAlreadyVotedForAnotherInCurrentTerm) {
            Raft.VoteResponse voteResponse = Raft.VoteResponse.newBuilder()
                    .setTerm(state.currentTerm())
                    .setVoteGranted(false)
                    .build();
            onVoteResponse.accept(voteResponse);
            return;
        }

        votedForMe = 0;
        leaderId = -1;
        if (Objects.equals(state.votedFor(), voteRequest.getCandidateId())) {
            if (state.currentTerm() < voteRequest.getTerm()) {
                replicatedLogManager.writePersistentState(
                        new PersistentState(voteRequest.getTerm(), voteRequest.getCandidateId())
                );
            }
            Raft.VoteResponse voteResponse = Raft.VoteResponse.newBuilder()
                    .setTerm(voteRequest.getTerm())
                    .setVoteGranted(true)
                    .build();

            onVoteResponse.accept(voteResponse);
            return;
        }

        LogId lastLogId = replicatedLogManager.readLastLogId();

        boolean isLogUpToDate
                = compareIdLogs(voteRequest.getLastLogTerm(), voteRequest.getLastLogIndex(), lastLogId) >= 0;

        if (!isLogUpToDate) { // new term, but old log, so we reject vote request and update our term
            replicatedLogManager.writePersistentState(new PersistentState(voteRequest.getTerm()));
            Raft.VoteResponse voteResponse = Raft.VoteResponse.newBuilder()
                    .setTerm(voteRequest.getTerm())
                    .setVoteGranted(false)
                    .build();
            onVoteResponse.accept(voteResponse);
            startTimeout(Timeout.ELECTION_TIMEOUT);
            return;
        }

        replicatedLogManager.writePersistentState(
                new PersistentState(voteRequest.getTerm(), voteRequest.getCandidateId())
        );
        Raft.VoteResponse voteResponse = Raft.VoteResponse.newBuilder()
                .setTerm(voteRequest.getTerm())
                .setVoteGranted(true)
                .build();

        onVoteResponse.accept(voteResponse);
        startTimeout(Timeout.ELECTION_TIMEOUT);
    }

    @Override
    public synchronized void onRequestVoteResult(int srcId, Raft.VoteResponse voteResponse) {
        MDC.put(MDC_NODE_ID, String.valueOf(nodeId));
        logger.info("Receive new VoteResponse {}", protobufMessageToString(voteResponse));

        long currentTerm = replicatedLogManager.readPersistentState().currentTerm();
        if (voteResponse.getTerm() < currentTerm) {
            return; // ignore obsolete messages
        }

        if (voteResponse.getTerm() > currentTerm) {
            leaderId = -1;
            replicatedLogManager.writePersistentState(new PersistentState(voteResponse.getTerm()));
            startTimeout(Timeout.ELECTION_TIMEOUT);
            return;
        }
        if (voteResponse.getVoteGranted()) {
            votedForMe++;
            if (votedForMe == quorum(config.getCluster().size())) {
                leaderId = nodeId;
                votedForMe = 0;
                onTimeout();
                while (!queue.isEmpty()) {
                    onClientCommand(queue.poll());
                }
            }
        }
        MDC.remove(MDC_NODE_ID);
    }

    @Override
    public void onClientCommandResult(Command command, CommandResult commandResult) {
        PersistentState state = replicatedLogManager.readPersistentState();
        int leaderId = (int) command.processId();
        if (commandResult.term() > state.currentTerm()) {
            externalGrpcActorInterface.onClientCommandResult(command, commandResult);
            replicatedLogManager.writePersistentState(new PersistentState(commandResult.term()));
            this.leaderId = leaderId;
            startTimeout(Timeout.ELECTION_TIMEOUT);
            while (!queue.isEmpty()) {
                command = queue.poll();
                internalGrpcActor.sendClientCommand(leaderId, command, this::onClientCommandResult);
            }
        } else {
            externalGrpcActorInterface.onClientCommandResult(command, commandResult);
        }
    }

    @Override
    public void onClientCommand(Command command) {
        if (leaderId == nodeId) {
            PersistentState state = replicatedLogManager.readPersistentState();
            LogId lastLogId = replicatedLogManager.readLastLogId();
            switch (command) {
                case InsertCommand insertCommand -> handleInsertCommandAsLeader(insertCommand, state, lastLogId);
                case GetCommand getCommand -> handleGetCommandAsLeader(getCommand, state, lastLogId);
            }
        } else if (leaderId != -1) {
            internalGrpcActor.sendClientCommand(leaderId, command, this::onClientCommandResult);
        } else {
            queue.add(command);
        }
    }

    private void handleInsertCommandAsLeader(InsertCommand command, PersistentState state, LogId lastLogId) {
        OperationType operationType = command.value() == null ? OperationType.DELETE : OperationType.PUT;
        Entry<MemorySegment> entry = createEntryFromInsertCommand(command);
        LogEntry<MemorySegment> logEntry = createLogEntryFromEntry(operationType, entry, lastLogId);

        if (command.uncommitted()) {
            CommandResult commandResult = stateMachine.apply(command, state.currentTerm());
            if (isClusterReadyToAcceptEntries(logEntry)) {
                sendAppendEntriesRequest(state, logEntry, lastLogId);
            }
            externalGrpcActorInterface.onClientCommandResult(command, commandResult);
        } else if (isClusterReadyToAcceptEntries(logEntry)) {
            sendAppendEntriesRequest(state, logEntry, lastLogId);
        }
        replicatedLogManager.appendLogEntry(logEntry);
    }

    private void handleGetCommandAsLeader(GetCommand command, PersistentState state, LogId lastLogId) {
        switch (command.readMode()) {
            case READ_CONSENSUS -> {
                Entry<MemorySegment> entry = new BaseEntry<>(
                        StringDaoWrapper.toMemorySegment(command.key()),
                        StringDaoWrapper.toMemorySegment(command.value()),
                        StringDaoWrapper.toMemorySegment(command.value()),
                        false
                );
                LogEntry<MemorySegment> logEntry = createLogEntryFromEntry(OperationType.GET, entry, lastLogId);

                if (isClusterReadyToAcceptEntries(logEntry)) {
                    sendAppendEntriesRequest(state, logEntry, lastLogId);
                }
                replicatedLogManager.appendLogEntry(logEntry);
            }

            case READ_LOCAL, READ_COMMITTED -> {
                CommandResult commandResult = stateMachine.apply(command, state.currentTerm());
                externalGrpcActorInterface.onClientCommandResult(command, commandResult);
            }

            default -> throw new IllegalArgumentException("UNRECOGNIZED readMode");
        }
    }

    private Entry<MemorySegment> createEntryFromInsertCommand(InsertCommand command) {
        return new BaseEntry<>(
                StringDaoWrapper.toMemorySegment(command.key()),
                StringDaoWrapper.toMemorySegment(command.value()),
                StringDaoWrapper.toMemorySegment(command.value()),
                command.uncommitted()
        );
    }

    private LogEntry<MemorySegment> createLogEntryFromEntry(
            OperationType operationType, Entry<MemorySegment> entry, LogId lastLogId
    ) {
        return new BaseLogEntry<>(
                operationType,
                entry,
                new LogId(lastLogId.term(), lastLogId.index() + 1)
        );
    }

    private boolean isClusterReadyToAcceptEntries(LogEntry<MemorySegment> logEntry) {
        int count = (int) IntStream.range(0, config.getCluster().size())
                .filter(i -> i != nodeId && nextIndex[i] == logEntry.logId().index())
                .count();

        return count >= quorum(config.getCluster().size());
    }

    private void sendAppendEntriesRequest(PersistentState state, LogEntry<MemorySegment> logEntry, LogId lastLogId) {
        Raft.AppendEntriesRequest appendEntriesRequest = Raft.AppendEntriesRequest.newBuilder()
                .setTerm(state.currentTerm())
                .setLeaderId(nodeId)
                .addEntries(EntryConverter.logEntryToRaftLogEntry(logEntry))
                .setPrevLogIndex(lastLogId.index())
                .setPrevLogTerm(lastLogId.term())
                .setLeaderCommit(replicatedLogManager.commitIndex())
                .build();
        internalGrpcActor.sendAppendEntriesRequest(appendEntriesRequest, this::onAppendEntryResult);
    }

    private void appendEntitiesIntoLog(Raft.AppendEntriesRequest request) {
        for (Raft.LogEntry entry : request.getEntriesList()) {
            replicatedLogManager.appendLogEntry(BaseLogEntry.valueOf(entry));
        }
    }
}
