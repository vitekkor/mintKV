package com.mint.db.impl;

import io.grpc.Status;
import io.grpc.StatusException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class RaftActor {
    private static final Logger log = LoggerFactory.getLogger(RaftActor.class);
    private static final Random rand = new Random();
    private static final int POOL_SIZE = 1;
    private final ScheduledExecutorService scheduledExecutor;
    private ScheduledFuture<?> scheduledFuture;
    private static final long HEARTBEAT_DELAY_MS = 5000L;
    private boolean amILeader = false; // ?

    public RaftActor() {
        this.scheduledExecutor = Executors.newScheduledThreadPool(POOL_SIZE);
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

}
