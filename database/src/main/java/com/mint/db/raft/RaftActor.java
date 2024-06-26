package com.mint.db.raft;

import com.mint.db.Raft;
import com.mint.db.config.annotations.CallbackKeeperBean;
import com.mint.db.config.annotations.EnvironmentBean;
import com.mint.db.config.annotations.InternalGrpcActorBean;
import com.mint.db.dao.Entry;
import com.mint.db.dao.impl.BaseEntry;
import com.mint.db.dao.impl.StringDaoWrapper;
import com.mint.db.grpc.InternalGrpcActorInterface;
import com.mint.db.http.server.CallbackKeeper;
import com.mint.db.raft.model.Command;
import com.mint.db.raft.model.CommandResult;
import com.mint.db.raft.model.GetCommand;
import com.mint.db.raft.model.InsertCommand;
import com.mint.db.raft.model.LogId;
import com.mint.db.raft.model.Pair;
import com.mint.db.replication.ReplicatedLogManager;
import com.mint.db.replication.model.LogEntry;
import com.mint.db.replication.model.PersistentState;
import com.mint.db.replication.model.impl.BaseLogEntry;
import com.mint.db.replication.model.impl.OperationType;
import com.mint.db.util.EntryConverter;
import com.mint.db.util.LogUtil;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
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
import static java.lang.Math.max;
import static java.lang.Math.min;

public class RaftActor implements RaftActorInterface {
    public static final String MDC_NODE_ID = "nodeId";
    private static final Logger logger = LoggerFactory.getLogger(RaftActor.class);
    private static final Random rand = new Random();
    private static final int POOL_SIZE = 1;
    private static final LogId START_LOG_ID = new LogId(0, 0);
    private final InternalGrpcActorInterface internalGrpcActor;
    private final Environment<MemorySegment> env;
    private final int nodeId;
    private final ScheduledExecutorService scheduledExecutor;
    private final Queue<Command> queue = new ArrayDeque<>();
    private final CallbackKeeper callbackKeeper;
    private ScheduledFuture<?> scheduledFuture;
    private int votedForMe = 0;
    private int leaderId = -1;
    private long nextTimeout = Long.MAX_VALUE;
    private final long[] nextIndex;
    private final long[] matchIndex;

    @Inject
    public RaftActor(
            @InternalGrpcActorBean InternalGrpcActorInterface internalGrpcActor,
            @EnvironmentBean Environment<MemorySegment> environment,
            @CallbackKeeperBean CallbackKeeper callbackKeeper
    ) {
        this.internalGrpcActor = internalGrpcActor;
        this.env = environment;

        this.callbackKeeper = callbackKeeper;
        this.scheduledExecutor = Executors.newScheduledThreadPool(POOL_SIZE);

        this.nodeId = env.nodeId();
        this.nextIndex = new long[env.numberOfProcesses()];
        for (int i = 0; i < env.numberOfProcesses(); i++) {
            nextIndex[i] = env.replicatedLogManager().readLastLogId().index() + 1;
        }
        this.matchIndex = new long[env.numberOfProcesses()];

        startTimeout(Timeout.ELECTION_TIMEOUT);
    }

    private static int quorum(final int clusterSize) {
        return clusterSize / 2;
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

    public void startTimeout(Timeout timeout) {
        long heartbeatTimeoutMs = env.config().getHeartbeatTimeoutMs();
        int numberOfProcesses = Math.max(env.numberOfProcesses(), 2);
        this.nextTimeout = heartbeatTimeoutMs + switch (timeout) {
            case ELECTION_TIMEOUT -> env.config().heartbeatRandom()
                    ? rand.nextLong(heartbeatTimeoutMs / numberOfProcesses, heartbeatTimeoutMs)
                    : (nodeId + 1) * heartbeatTimeoutMs / numberOfProcesses;
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
    public synchronized void onTimeout() {
        MDC.put(MDC_NODE_ID, String.valueOf(nodeId));
        logger.debug("On timeout; leaderId = {}", leaderId);
        if (leaderId == nodeId) { // send heartbeat
            logger.debug("Send heartbeats");

            PersistentState state = env.replicatedLogManager().readPersistentState();
            for (int i = 0; i < env.numberOfProcesses(); i++) {
                if (i != nodeId) {
                    // if follower lagging behind from our log
                    // then we send nextIndex entry to him
                    var lastAppliedIndex = env.replicatedLogManager().lastAppliedIndex();
                    if (nextIndex[i] <= lastAppliedIndex) { // todo send batch
                        var logEntry = env.replicatedLogManager().readLog(nextIndex[i] - 1);
                        LogId prevLogId = logEntry != null ? logEntry.logId() : START_LOG_ID;
                        LogEntry<MemorySegment> nextEntry = env.replicatedLogManager().readLog(nextIndex[i]);
                        Raft.AppendEntriesRequest.Builder builder = Raft.AppendEntriesRequest.newBuilder()
                                .setTerm(state.currentTerm())
                                .setLeaderId(nodeId)
                                .setPrevLogIndex(prevLogId.index())
                                .setPrevLogTerm(prevLogId.term())
                                .setLeaderCommit(env.replicatedLogManager().commitIndex());
                        if (nextEntry != null) {
                            builder.addEntries(EntryConverter.logEntryToRaftLogEntry(nextEntry));
                        }
                        internalGrpcActor.sendAppendEntriesRequest(i, builder.build(), this::onAppendEntryResult);
                    } else {
                        LogId logId = env.replicatedLogManager().readLastLogId();
                        Raft.AppendEntriesRequest appendEntriesRequest = Raft.AppendEntriesRequest.newBuilder()
                                .setTerm(state.currentTerm())
                                .setLeaderId(nodeId)
                                .setPrevLogIndex(logId.index())
                                .setPrevLogTerm(logId.term())
                                .setLeaderCommit(env.replicatedLogManager().commitIndex())
                                .build();
                        internalGrpcActor.sendAppendEntriesRequest(i, appendEntriesRequest, this::onAppendEntryResult);
                    }
                }
            }
            startTimeout(Timeout.LEADER_HEARTBEAT_PERIOD);
        } else { // become a candidate
            logger.debug("Become a candidate");
            PersistentState oldState = env.replicatedLogManager().readPersistentState();

            PersistentState state = new PersistentState(oldState.currentTerm() + 1, nodeId);
            env.replicatedLogManager().writePersistentState(state);
            votedForMe = 0;
            leaderId = -1;

            startTimeout(Timeout.ELECTION_TIMEOUT);

            LogId lastLogId = env.replicatedLogManager().readLastLogId();
            Raft.VoteRequest voteRequest = Raft.VoteRequest.newBuilder()
                    .setCandidateId(nodeId)
                    .setTerm(state.currentTerm())
                    .setLastLogIndex(lastLogId.index())
                    .setLastLogTerm(lastLogId.term())
                    .build();
            internalGrpcActor.sendVoteRequest(voteRequest, this::onRequestVoteResult);
        }
        MDC.remove(MDC_NODE_ID);
    }

    @Override
    public synchronized void onAppendEntry(
            Raft.AppendEntriesRequest appendEntriesRequest,
            Consumer<Raft.AppendEntriesResponse> onAppendEntriesResponse
    ) {
        MDC.put(MDC_NODE_ID, String.valueOf(nodeId));
        logger.debug("Received AppendEntriesRequest: {}", LogUtil.protobufMessageToString(appendEntriesRequest));
        PersistentState state = env.replicatedLogManager().readPersistentState();
        LogId prevLogId = env.replicatedLogManager().readLastLogId();

        if (appendEntriesRequest.getTerm() < state.currentTerm()) { // reject obsolete messages
            logger.debug("Rejecting AppendEntriesRequest due to obsolete term.");
            Raft.AppendEntriesResponse appendEntriesResponse = Raft.AppendEntriesResponse.newBuilder()
                    .setTerm(state.currentTerm())
                    .setLastIndex(-1)
                    .build();
            onAppendEntriesResponse.accept(appendEntriesResponse);
            return;
        }
        LogId appendEntriesRequestPervLogId
                = new LogId(appendEntriesRequest.getPrevLogIndex(), appendEntriesRequest.getPrevLogTerm());

        if (compareIdLogs(prevLogId, appendEntriesRequestPervLogId) < 0) {
            // our log has not up-to-date,
            // so we are sending null to force the leader to send us the missing log
            logger.debug("Log is not up-to-date. Requesting missing log from leader.");
            Raft.AppendEntriesResponse appendEntriesResponse = Raft.AppendEntriesResponse.newBuilder()
                    .setTerm(appendEntriesRequest.getTerm())
                    .setLastIndex(-1)
                    .build();
            onAppendEntriesResponse.accept(appendEntriesResponse);
            env.replicatedLogManager().writePersistentState(new PersistentState(appendEntriesRequest.getTerm()));
            startTimeout(Timeout.ELECTION_TIMEOUT);
            return;
        }

        // if we don't know who leader is, we decide that first node that sends us AppendEntryRPC is the leader
        if (leaderId == -1) {
            leaderId = appendEntriesRequest.getLeaderId();
        }

        if (appendEntriesRequest.getEntriesCount() == 0) {
            logger.debug("Received heartbeat from leader {}", appendEntriesRequest.getLeaderId());
            onHeartBeat(appendEntriesRequest, state, onAppendEntriesResponse);
            return;
        }
        long term = Math.max(appendEntriesRequest.getTerm(), state.currentTerm());
        Raft.LogEntry lastEntry = appendEntriesRequest.getEntries(appendEntriesRequest.getEntriesCount() - 1);
        long index = Math.max(lastEntry.getIndex(), prevLogId.index());

        Raft.AppendEntriesResponse appendEntriesResponse = Raft.AppendEntriesResponse.newBuilder()
                .setTerm(term)
                .setLastIndex(index)
                .build();

        onAppendEntriesResponse.accept(appendEntriesResponse);

        if (appendEntriesResponse.getTerm() > state.currentTerm()) {
            // new leader is, our log is up-to-date, so we update our state
            logger.debug("New leader detected. Updating state.");
            env.replicatedLogManager().writePersistentState(new PersistentState(appendEntriesRequest.getTerm()));
            leaderId = -1;
        }

        LogId lastEntryLogId = new LogId(lastEntry.getIndex(), lastEntry.getTerm());

        // in the case when the leader thinks that we are lagging behind
        // we shouldn't rewrite our log
        if (compareIdLogs(lastEntryLogId, prevLogId) > 0) {
            logger.debug("Appending entries into log.");
            appendEntitiesIntoLog(appendEntriesRequest);
        }
        startTimeout(Timeout.ELECTION_TIMEOUT);

        long lastCommitIndex = env.replicatedLogManager().commitIndex();

        // commit all entries that were appended by us and committed by leader
        if (appendEntriesRequest.getLeaderCommit() > lastCommitIndex) {
            long newCommitIndex
                    = min(appendEntriesRequest.getLeaderCommit(), env.replicatedLogManager().lastAppliedIndex());
            commitEntries(state, lastCommitIndex, newCommitIndex);
        }
        MDC.remove(MDC_NODE_ID);
    }

    private void commitEntries(PersistentState state, long lastCommitIndex, long newCommitIndex) {
        ReplicatedLogManager<MemorySegment> replicatedLogManager = env.replicatedLogManager();
        synchronized (replicatedLogManager) {
            replicatedLogManager.setCommitIndex(newCommitIndex);
            logger.debug("Committing entries up to index {}", newCommitIndex);
            for (long i = lastCommitIndex + 1; i <= newCommitIndex; i++) {
                LogEntry<MemorySegment> logEntry = replicatedLogManager.readLog(i);
                env.stateMachine().apply(logEntry.getCommand(), state.currentTerm());
            }
        }
    }

    private void onHeartBeat(
            Raft.AppendEntriesRequest appendEntriesRequest,
            PersistentState state,
            Consumer<Raft.AppendEntriesResponse> onVoteResponse
    ) {
        long lastCommitIndex = env.replicatedLogManager().commitIndex();
        logger.debug(
                "Current commit index: {}, Leader commit index: {}",
                lastCommitIndex,
                appendEntriesRequest.getLeaderCommit()
        );
        // commit all entries that were appended by us and committed by leader
        if (lastCommitIndex < appendEntriesRequest.getLeaderCommit()) {
            var lastAppliedIndex = env.replicatedLogManager().lastAppliedIndex();
            long newCommitIndex = min(lastAppliedIndex, appendEntriesRequest.getLeaderCommit());
            commitEntries(state, lastCommitIndex, newCommitIndex);
        }

        if (appendEntriesRequest.getTerm() > state.currentTerm()) {
            // new leader is, our log is up-to-date, so we update our state
            logger.debug("New leader detected with higher term. Updating state.");
            PersistentState newState = new PersistentState(appendEntriesRequest.getTerm());
            Raft.AppendEntriesResponse appendEntriesResponse = Raft.AppendEntriesResponse.newBuilder()
                    .setTerm(newState.currentTerm())
                    .setLastIndex(env.replicatedLogManager().readLastLogId().index())
                    .build();
            onVoteResponse.accept(appendEntriesResponse);
            env.replicatedLogManager().writePersistentState(newState);
        } else { // just reply
            logger.debug("Replying to heartbeat with current state.");
            Raft.AppendEntriesResponse appendEntriesResponse = Raft.AppendEntriesResponse.newBuilder()
                    .setTerm(state.currentTerm())
                    .setLastIndex(env.replicatedLogManager().readLastLogId().index())
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
    public synchronized void onAppendEntryResult(int srcId, Raft.AppendEntriesResponse appendEntriesResponse) {
        MDC.put(MDC_NODE_ID, String.valueOf(nodeId));
        logger.debug(
                "Received appendEntriesResponse from {}: {}",
                srcId,
                LogUtil.protobufMessageToString(appendEntriesResponse)
        );
        PersistentState currentState = env.replicatedLogManager().readPersistentState();

        // ignore obsolete messages
        if (appendEntriesResponse.getTerm() < currentState.currentTerm()) {
            logger.debug("onAppendEntryResult: ignore obsolete messages");
            return;
        }

        // become a follower
        if (appendEntriesResponse.getTerm() > currentState.currentTerm()) {
            logger.debug("AppendEntriesResponse term higher then our one. Become a follower");
            leaderId = -1;
            env.replicatedLogManager().writePersistentState(new PersistentState(appendEntriesResponse.getTerm()));
            startTimeout(Timeout.ELECTION_TIMEOUT);
            return;
        }

        // handle result
        boolean success = appendEntriesResponse.getLastIndex() != -1;
        if (success) {
            onSuccessfullAppendEntryResult(srcId, appendEntriesResponse.getLastIndex(), currentState);
        } else {
            onFailedAppendEntryResult(srcId);
        }
        MDC.remove(MDC_NODE_ID);
    }

    private void onSuccessfullAppendEntryResult(int srcId, long messageLastIndex, PersistentState state) {
        // update indexes
        nextIndex[srcId] = messageLastIndex + 1;
        matchIndex[srcId] = messageLastIndex;

        // commit entries
        for (long index = env.replicatedLogManager().commitIndex() + 1; index <= messageLastIndex; index++) {
            int nodesCount = 0;
            for (long i : matchIndex) {
                if (i >= index) {
                    nodesCount++;
                }
            }


            LogEntry<MemorySegment> logEntry = env.replicatedLogManager().readLog(index);
            boolean isLogTermEqualToCurrentTerm = logEntry != null && logEntry.logId().term() == state.currentTerm();
            if (nodesCount == quorum(env.numberOfProcesses()) && isLogTermEqualToCurrentTerm) {
                long commitIndex = env.replicatedLogManager().commitIndex();
                logger.debug("Commit entries from {} to {}", commitIndex + 1, index);
                Collection<Pair<Command, CommandResult>> leaderResults = new ArrayList<>();

                for (long i = commitIndex + 1; i <= index; i++) {
                    LogEntry<MemorySegment> logEntryMemorySegment = env.replicatedLogManager().readLog(i);
                    Command command = logEntryMemorySegment.getCommand();

                    logger.debug("Apply command {}", command);
                    CommandResult commandResult = env.stateMachine().apply(command, state.currentTerm());
                    if (leaderId == nodeId
                            && logEntryMemorySegment.logId().term() == state.currentTerm()
                            && command.processId() == nodeId
                    ) {
                        leaderResults.add(new Pair<>(command, commandResult));
                    } else if (leaderId == nodeId && logEntryMemorySegment.logId().term() == state.currentTerm()) {
                        internalGrpcActor.onClientCommandResult(command, commandResult);
                    }
                }

                // send results to client
                for (Pair<Command, CommandResult> leaderResult : leaderResults) {
                    callbackKeeper.onClientCommandResult(leaderResult.first(), leaderResult.second());
                }

                // update commit index
                env.replicatedLogManager().setCommitIndex(index);
            }
        }
    }


    private void onFailedAppendEntryResult(int srcId) {
        logger.error("onFailedAppendEntryResult: {}, {}", srcId, nextIndex[srcId]);
        nextIndex[srcId] = max(0, nextIndex[srcId] - 1);

        if (leaderId != nodeId) {
            return;
        }

        LogEntry<MemorySegment> logEntry = env.replicatedLogManager().readLog(nextIndex[srcId]);

        long prevLogIndex = Math.max(nextIndex[srcId] - 1, 0);
        LogEntry<MemorySegment> prevLogEntry = env.replicatedLogManager().readLog(prevLogIndex);
        LogId prevLogId = prevLogEntry != null ? prevLogEntry.logId() : START_LOG_ID;

        Raft.AppendEntriesRequest.Builder builder = Raft.AppendEntriesRequest.newBuilder()
                .setTerm(env.replicatedLogManager().readPersistentState().currentTerm())
                .setPrevLogIndex(prevLogId.index())
                .setPrevLogTerm(prevLogId.term())
                .setLeaderCommit(env.replicatedLogManager().commitIndex())
                .setLeaderId(nodeId);
        if (logEntry != null) {
            builder.addEntries(EntryConverter.logEntryToRaftLogEntry(logEntry));
        }
        internalGrpcActor.sendAppendEntriesRequest(srcId, builder.build(), this::onAppendEntryResult);
    }

    public synchronized void onRequestVote(Raft.VoteRequest voteRequest, Consumer<Raft.VoteResponse> onVoteResponse) {
        MDC.put(MDC_NODE_ID, String.valueOf(nodeId));
        logger.info("Receive new VoteRequest {}", protobufMessageToString(voteRequest));

        PersistentState state = env.replicatedLogManager().readPersistentState();
        // reject old term
        if (voteRequest.getTerm() < state.currentTerm()) {
            logger.debug("Reject old term");
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
            logger.debug("Reject new leader in the same term");
            Raft.VoteResponse voteResponse = Raft.VoteResponse.newBuilder()
                    .setTerm(state.currentTerm())
                    .setVoteGranted(false)
                    .build();
            onVoteResponse.accept(voteResponse);
            return;
        }

        LogId lastLogId = env.replicatedLogManager().readLastLogId();

        boolean isLogUpToDate
                = compareIdLogs(voteRequest.getLastLogTerm(), voteRequest.getLastLogIndex(), lastLogId) >= 0;

        votedForMe = 0;
        if (Objects.equals(state.votedFor(), voteRequest.getCandidateId()) && isLogUpToDate) {
            logger.debug("Vote for {}", voteRequest.getCandidateId());
            leaderId = -1;
            if (state.currentTerm() < voteRequest.getTerm()) {
                env.replicatedLogManager().writePersistentState(
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

        if (!isLogUpToDate) { // new term, but old log, so we reject vote request and update our term
            logger.debug("New term, but old log, so we reject vote request and update our term");
            env.replicatedLogManager().writePersistentState(new PersistentState(voteRequest.getTerm()));
            Raft.VoteResponse voteResponse = Raft.VoteResponse.newBuilder()
                    .setTerm(voteRequest.getTerm())
                    .setVoteGranted(false)
                    .build();
            onVoteResponse.accept(voteResponse);
            startTimeout(Timeout.ELECTION_TIMEOUT);
            return;
        }

        logger.debug("Vote for {}", voteRequest.getCandidateId());
        env.replicatedLogManager().writePersistentState(
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
        logger.info("Receive new VoteResponse from {} {}", srcId, protobufMessageToString(voteResponse));

        long currentTerm = env.replicatedLogManager().readPersistentState().currentTerm();
        if (voteResponse.getTerm() < currentTerm) {
            return; // ignore obsolete messages
        }

        if (voteResponse.getTerm() > currentTerm) {
            leaderId = -1;
            env.replicatedLogManager().writePersistentState(new PersistentState(voteResponse.getTerm()));
            startTimeout(Timeout.ELECTION_TIMEOUT);
            return;
        }
        if (voteResponse.getVoteGranted()) {
            votedForMe++;
            if (votedForMe == quorum(env.numberOfProcesses())) {
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
    public synchronized void onClientCommandResult(int srcId, Command command, CommandResult commandResult) {
        MDC.put(MDC_NODE_ID, String.valueOf(nodeId));
        logger.debug("client command result received: {}, {}", command, commandResult);

        PersistentState state = env.replicatedLogManager().readPersistentState();
        int leaderId = srcId;
        if (commandResult.term() > state.currentTerm()) {
            logger.debug("Command result with newest term received. Became a follower");
            sendCommandResult(command, commandResult);
            env.replicatedLogManager().writePersistentState(new PersistentState(commandResult.term()));
            this.leaderId = leaderId;
            startTimeout(Timeout.ELECTION_TIMEOUT);
            while (!queue.isEmpty()) {
                command = queue.poll();
                internalGrpcActor.sendClientCommand(leaderId, command, this::onClientCommandResult);
            }
        } else {
            sendCommandResult(command, commandResult);
        }
        MDC.remove(MDC_NODE_ID);
    }

    @Override
    public synchronized void onClientCommand(Command command) {
        MDC.put(MDC_NODE_ID, String.valueOf(nodeId));
        logger.debug("Received client command {}", command);
        if (leaderId == nodeId) {
            PersistentState state = env.replicatedLogManager().readPersistentState();
            LogId lastLogId = env.replicatedLogManager().readLastLogId();
            switch (command) {
                case InsertCommand insertCommand -> handleInsertCommandAsLeader(insertCommand, state, lastLogId);
                case GetCommand getCommand -> handleGetCommandAsLeader(getCommand, state, lastLogId);
            }
        } else if (leaderId != -1) {
            logger.debug("Send command {} to leader {}", command, leaderId);
            internalGrpcActor.sendClientCommand(leaderId, command, this::onClientCommandResult);
        } else {
            logger.debug("Add command to queue");
            queue.add(command);
        }
        MDC.remove(MDC_NODE_ID);
    }

    private void handleInsertCommandAsLeader(InsertCommand command, PersistentState state, LogId lastLogId) {
        OperationType operationType = command.value() == null ? OperationType.DELETE : OperationType.PUT;
        Entry<MemorySegment> entry = createEntryFromInsertCommand(command);
        LogEntry<MemorySegment> logEntry =
                createLogEntryFromEntry(operationType, entry, lastLogId, state.currentTerm(), command.processId());

        if (command.uncommitted()) {
            CommandResult commandResult = env.stateMachine().apply(command, state.currentTerm());
            if (isClusterReadyToAcceptEntries(logEntry)) {
                sendAppendEntriesRequest(state, logEntry, lastLogId);
            }
            sendCommandResult(command, commandResult);
        } else if (isClusterReadyToAcceptEntries(logEntry)) {
            sendAppendEntriesRequest(state, logEntry, lastLogId);
        }
        env.replicatedLogManager().appendLogEntry(logEntry);
    }

    private void handleGetCommandAsLeader(GetCommand command, PersistentState state, LogId lastLogId) {
        switch (command.readMode()) {
            case READ_CONSENSUS -> {
                Entry<MemorySegment> entry = createEntryFromGetCommand(command);
                LogEntry<MemorySegment> logEntry = createLogEntryFromEntry(
                        OperationType.GET,
                        entry,
                        lastLogId,
                        state.currentTerm(),
                        command.processId()
                );

                if (isClusterReadyToAcceptEntries(logEntry)) {
                    sendAppendEntriesRequest(state, logEntry, lastLogId);
                }
                env.replicatedLogManager().appendLogEntry(logEntry);
            }

            case READ_LOCAL, READ_COMMITTED -> {
                CommandResult commandResult = env.stateMachine().apply(command, state.currentTerm());
                sendCommandResult(command, commandResult);
            }

            default -> throw new IllegalArgumentException("UNRECOGNIZED readMode");
        }
    }

    private void sendCommandResult(Command command, CommandResult commandResult) {
        if (command.processId() == nodeId) {
            callbackKeeper.onClientCommandResult(command, commandResult);
        } else {
            internalGrpcActor.onClientCommandResult(command, commandResult);
        }
    }

    private Entry<MemorySegment> createEntryFromGetCommand(GetCommand command) {
        return new BaseEntry(
                StringDaoWrapper.toMemorySegment(command.key()),
                null,
                null,
                false
        );
    }

    private Entry<MemorySegment> createEntryFromInsertCommand(InsertCommand command) {
        return new BaseEntry(
                StringDaoWrapper.toMemorySegment(command.key()),
                StringDaoWrapper.toMemorySegment(command.value()),
                StringDaoWrapper.toMemorySegment(command.value()),
                command.uncommitted()
        );
    }

    private static LogEntry<MemorySegment> createLogEntryFromEntry(
            OperationType operationType,
            Entry<MemorySegment> entry,
            LogId lastLogId,
            long currentTerm,
            int processId
    ) {
        return new BaseLogEntry<>(
                operationType,
                entry,
                new LogId(lastLogId.index() + 1, currentTerm),
                processId
        );
    }

    private boolean isClusterReadyToAcceptEntries(LogEntry<MemorySegment> logEntry) {
        int count = (int) IntStream.range(0, env.numberOfProcesses())
                .filter(i -> i != nodeId && nextIndex[i] == logEntry.logId().index())
                .count();

        return count >= quorum(env.numberOfProcesses());
    }

    private void sendAppendEntriesRequest(PersistentState state, LogEntry<MemorySegment> logEntry, LogId lastLogId) {
        Raft.AppendEntriesRequest appendEntriesRequest = Raft.AppendEntriesRequest.newBuilder()
                .setTerm(state.currentTerm())
                .setLeaderId(nodeId)
                .addEntries(EntryConverter.logEntryToRaftLogEntry(logEntry))
                .setPrevLogIndex(lastLogId.index())
                .setPrevLogTerm(lastLogId.term())
                .setLeaderCommit(env.replicatedLogManager().commitIndex())
                .build();
        internalGrpcActor.sendAppendEntriesRequest(appendEntriesRequest, this::onAppendEntryResult);
    }

    private void appendEntitiesIntoLog(Raft.AppendEntriesRequest request) {
        ReplicatedLogManager<MemorySegment> replicatedLogManager = env.replicatedLogManager();
        for (Raft.LogEntry entry : request.getEntriesList()) {
            replicatedLogManager.appendLogEntry(BaseLogEntry.valueOf(entry));
        }
    }

    public void close() throws IOException {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(true);
        }
        scheduledExecutor.close();
        env.replicatedLogManager().close();
    }
}
