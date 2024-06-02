package com.mint.db.raft;

import com.mint.db.Raft;
import com.mint.db.config.NodeConfig;
import com.mint.db.grpc.InternalGrpcActor;
import com.mint.db.grpc.InternalGrpcActorInterface;
import com.mint.db.raft.model.Command;
import com.mint.db.raft.model.CommandResult;
import com.mint.db.raft.model.LogId;
import com.mint.db.replication.ReplicatedLogManager;
import com.mint.db.replication.impl.ReplicatedLogManagerImpl;
import com.mint.db.replication.model.LogEntry;
import com.mint.db.replication.model.Message;
import com.mint.db.replication.model.PersistentState;
import com.mint.db.replication.model.impl.BaseLogEntry;
import com.mint.db.replication.model.impl.FollowerMessage;
import com.mint.db.replication.model.impl.LeaderMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.lang.foreign.MemorySegment;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.mint.db.util.LogUtil.protobufMessageToString;

public class RaftActor implements RaftActorInterface {
    private static final Logger logger = LoggerFactory.getLogger(RaftActor.class);
    private static final Random rand = new Random();
    private static final int POOL_SIZE = 1;
    public static final String MDC_NODE_ID = "nodeId";

    private final InternalGrpcActorInterface internalGrpcActor;
    private final int nodeId;
    private final ScheduledExecutorService scheduledExecutor;
    private ScheduledFuture<?> scheduledFuture;
    private final ReplicatedLogManager<MemorySegment> replicatedLogManager;
    private final NodeConfig config;
    private final AtomicBoolean amILeader = new AtomicBoolean();
    private int votedForMe = 0;
    private int leaderId = -1;
    private final Queue<Command> queue = new ArrayDeque<>();
    private long nextTimeout = Long.MAX_VALUE;

    public RaftActor(InternalGrpcActorInterface internalGrpcActor, NodeConfig config, PersistentState state) {
        this.scheduledExecutor = Executors.newScheduledThreadPool(POOL_SIZE);
        this.replicatedLogManager = new ReplicatedLogManagerImpl(config, state);
        this.config = config;
        this.internalGrpcActor = internalGrpcActor;
        this.nodeId = config.getNodeId();

        startTimeout(Timeout.ELECTION_TIMEOUT);
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

    public Collection<Message> appendEntities(Raft.AppendEntriesRequest request) {
        if (amILeader.getOpaque()) {
            appendEntitiesIntoLog(request);
            Collection<Message> messages = new LinkedList<>();
            for (int followerId = 0; followerId < config.getCluster().size(); followerId++) {
                if (followerId != config.getNodeId()) {
                    messages.add(
                            new FollowerMessage(
                                    config.getCluster().get(followerId),
                                    request
                            )
                    );
                }
            }
            return messages;
        } else {
            return List.of(
                    new LeaderMessage(
                            config.getCluster().get(request.getLeaderId()),
                            request
                    )
            );
        }
    }

    @Override
    public void onTimeout() {
        if (leaderId == nodeId) { // send heartbeat
            PersistentState state = replicatedLogManager.readPersistentState();
            LogId logId = replicatedLogManager.readLastLogId();
            Raft.AppendEntriesRequest appendEntriesRequest = Raft.AppendEntriesRequest.newBuilder()
                    .setTerm(state.currentTerm())
                    .setLeaderId(nodeId)
                    .setPrevLogIndex(logId.index())
                    .setPrevLogTerm(logId.term())
                    .setLeaderCommit(replicatedLogManager.commitIndex())
                    .build();
            internalGrpcActor.sendAppendEntriesRequest(appendEntriesRequest, this::onAppendEntryResult);
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
                = comparePrevLogs(voteRequest.getLastLogTerm(), voteRequest.getLastLogIndex(), lastLogId) >= 0;

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
    public void onClientCommand(Command command) {
        // TODO
    }

    @Override
    public void onClientCommandResult(CommandResult commandResult) {
        // TODO
    }

    private void appendEntitiesIntoLog(Raft.AppendEntriesRequest request) {
        for (LogEntry entry : request.getEntriesList()) {
            replicatedLogManager.appendLogEntry(BaseLogEntry.valueOf(entry));
        }
    }

    private static int quorum(final int clusterSize) {
        return clusterSize / 2 + 1;
    }

    private static int comparePrevLogs(long term, long index, LogId logId) {
        if (term != logId.term()) {
            return Long.compare(term, logId.term());
        }
        return Long.compare(index, logId.index());
    }
}
