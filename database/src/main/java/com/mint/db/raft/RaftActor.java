package com.mint.db.raft;

import com.mint.db.Raft;
import com.mint.db.config.NodeConfig;
import com.mint.db.grpc.InternalGrpcActor;
import com.mint.db.replication.ReplicatedLogManager;
import com.mint.db.replication.impl.ReplicatedLogManagerImpl;
import com.mint.db.replication.model.LogEntry;
import com.mint.db.replication.model.Message;
import com.mint.db.replication.model.impl.BaseLogEntry;
import com.mint.db.replication.model.impl.FollowerMessage;
import com.mint.db.replication.model.impl.LeaderMessage;
import io.grpc.Status;
import io.grpc.StatusException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.lang.foreign.MemorySegment;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mint.db.util.LogUtil.protobufMessageToString;

public class RaftActor {
    private static final Logger logger = LoggerFactory.getLogger(RaftActor.class);
    private static final Random rand = new Random();
    private static final int POOL_SIZE = 1;
    private static final long HEARTBEAT_DELAY_MS = 5000L;
    public static final String NODE_ID = "nodeId";

    private final InternalGrpcActor internalGrpcActor;
    private final int nodeId;
    private final ScheduledExecutorService scheduledExecutor;
    private ScheduledFuture<?> scheduledFuture;
    private final ReplicatedLogManager<MemorySegment> replicatedLogManager;
    private final NodeConfig config;
    private final LinkedList<LogEntry<MemorySegment>> log = new LinkedList<>();
    private final AtomicBoolean amILeader = new AtomicBoolean();
    private long currentTerm = 0;
    private long votedFor = -1;
    private int votedForMe = 0;

    public RaftActor(InternalGrpcActor internalGrpcActor, NodeConfig config) {
        this.scheduledExecutor = Executors.newScheduledThreadPool(POOL_SIZE);
        this.replicatedLogManager = new ReplicatedLogManagerImpl(config);
        this.config = config;
        this.internalGrpcActor = internalGrpcActor;
        this.nodeId = config.getNodeId();

        sleepBeforeElections();
    }

    private void sleepBeforeElections() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }

        scheduledFuture = scheduledExecutor.schedule(
                this::onHeartBeatNotReceived,
                HEARTBEAT_DELAY_MS + rand.nextLong(1000L, 5000L),
                TimeUnit.MILLISECONDS
        );
    }

    private synchronized void onHeartBeatNotReceived() {
        MDC.put(NODE_ID, String.valueOf(nodeId));
        logger.info("HeartBeat not received.");
        votedFor = nodeId;
        currentTerm++;
        Raft.VoteRequest voteRequest = Raft.VoteRequest.newBuilder()
                .setTerm(currentTerm)
                .setCandidateId(nodeId)
                .setLastLogIndex(log.size())
                .setLastLogTerm(log.isEmpty() ? -1 : log.getLast().term())
                .build();

        logger.info("Send new vote request {}", protobufMessageToString(voteRequest));
        internalGrpcActor.onLeaderCandidate(voteRequest);
        sleepBeforeElections();
    }

    public void onHeartBeat() throws StatusException {
        MDC.put(NODE_ID, String.valueOf(nodeId));
        if (amILeader.getOpaque()) {
            throw new StatusException(Status.FAILED_PRECONDITION);
        }

        logger.info("heartbeat");
        sleepBeforeElections();
    }

    private void sendHeartBeats() {
        Raft.AppendEntriesRequest appendEntriesRequest = Raft.AppendEntriesRequest.newBuilder()
                .setTerm(currentTerm)
                .setLeaderId(nodeId)
                .setPrevLogIndex(log.isEmpty() ? -1 : log.size())
                .setPrevLogTerm(currentTerm - 1)
                .setLeaderCommit(log.size()) // todo индекс записи, до которой данные уже зафиксированы
                .build();

        internalGrpcActor.onAppendEntityRequest(appendEntriesRequest);
        scheduledFuture = scheduledExecutor.schedule(
                this::sendHeartBeats,
                HEARTBEAT_DELAY_MS + rand.nextLong(1000L, 5000L),
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

    public synchronized Raft.VoteResponse onRequestVote(Raft.VoteRequest request) {
        MDC.put(NODE_ID, String.valueOf(nodeId));
        logger.info("Receive new VoteRequest {}", protobufMessageToString(request));

        Raft.VoteResponse.Builder responseBuilder = Raft.VoteResponse.newBuilder();

        // check request term
        if (request.getTerm() < currentTerm) {
            logger.info("VoteRequest term is too old.");
            return responseBuilder
                    .setTerm(currentTerm)
                    .setVoteGranted(false)
                    .build();
        }

        // check whether node has already voted for another candidate this term
        boolean isAlreadyVotedForAnother = (votedFor != -1 && votedFor != request.getCandidateId());

        // check the relevance of the candidate log
        LogEntry<MemorySegment> lastLogEntry = log.isEmpty() ? null : log.getLast();
        boolean isLogUpToDate = (lastLogEntry == null)
                || (request.getLastLogTerm() > lastLogEntry.term())
                || (request.getLastLogTerm() == lastLogEntry.term() && request.getLastLogIndex() >= log.size() - 1);

        // new term or node has not voted for another and their log is up-to-date (voteRequest retry)
        if (request.getTerm() > currentTerm || !isAlreadyVotedForAnother && isLogUpToDate) {
            logger.info("VoteRequest has been accepted.");
            currentTerm = request.getTerm();
            votedFor = request.getCandidateId();
            sleepBeforeElections();

            return responseBuilder
                    .setTerm(currentTerm)
                    .setVoteGranted(true)
                    .build();
        } else {
            logger.info("VoteRequest has been rejected.");
            return responseBuilder
                    .setTerm(currentTerm)
                    .setVoteGranted(false)
                    .build();
        }
    }

    public synchronized void onVoteResponse(Raft.VoteResponse voteResponse) {
        MDC.put(NODE_ID, String.valueOf(nodeId));
        logger.info("Receive new VoteResponse {}", protobufMessageToString(voteResponse));
        if (voteResponse.getVoteGranted()) {
            votedForMe++;
            if (votedForMe > quorum(config.getCluster().size())) {
                amILeader.setPlain(true);

                if (scheduledFuture != null) {
                    scheduledFuture.cancel(false);
                }
                sendHeartBeats();
            }
        }
        MDC.remove(NODE_ID);
    }

    private void appendEntitiesIntoLog(Raft.AppendEntriesRequest request) {
        for (Raft.LogEntry entry : request.getEntriesList()) {
            replicatedLogManager.appendLogEntry(BaseLogEntry.valueOf(entry));
        }
    }

    private static int quorum(final int clusterSize) {
        return clusterSize / 2 + 1;
    }
}
