package com.mint.db.impl;

import com.mint.db.Raft;
import com.mint.db.config.NodeConfig;
import com.mint.db.replication.ReplicatedLogManager;
import com.mint.db.replication.impl.ReplicatedLogManagerImpl;
import com.mint.db.replication.model.Message;
import com.mint.db.replication.model.impl.BaseLogEntry;
import com.mint.db.replication.model.impl.FollowerMessage;
import com.mint.db.replication.model.impl.LeaderMessage;
import io.grpc.Status;
import io.grpc.StatusException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class RaftActor {
    private static final Logger log = LoggerFactory.getLogger(RaftActor.class);
    private static final Random rand = new Random();
    private static final int POOL_SIZE = 1;
    private static final long HEARTBEAT_DELAY_MS = 5000L;
    private final ScheduledExecutorService scheduledExecutor;
    private final ReplicatedLogManager replicatedLogManager;
    private final NodeConfig config;
    private ScheduledFuture<?> scheduledFuture;
    private boolean amILeader = false; // ?

    public RaftActor(NodeConfig config) {
        this.scheduledExecutor = Executors.newScheduledThreadPool(POOL_SIZE);
        this.replicatedLogManager = new ReplicatedLogManagerImpl(config);
        this.config = config;
    }

    private void sleepBeforeElections() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }

        scheduledFuture = scheduledExecutor.scheduleAtFixedRate(
                this::onHeartBeatNotReceived,
                HEARTBEAT_DELAY_MS,
                HEARTBEAT_DELAY_MS + rand.nextLong(1000L, 5000L),
                TimeUnit.MILLISECONDS);

    }

    private void onHeartBeatNotReceived() {
        System.out.println("STUB");
    }

    public void onHeartBeat() throws StatusException {
        if (amILeader) {
            throw new StatusException(Status.FAILED_PRECONDITION);
        }

        log.info("heartbeat");
        sleepBeforeElections();
    }

    public Collection<Message> appendEntities(Raft.AppendEntriesRequest request) {
        if (amILeader) {
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

    private void appendEntitiesIntoLog(Raft.AppendEntriesRequest request) {
        for (Raft.LogEntry entry : request.getEntriesList()) {
            replicatedLogManager.appendLogEntry(BaseLogEntry.valueOf(entry));
        }
    }
}
