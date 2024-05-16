package com.mint.db.grpc.client;

import com.mint.db.Raft;
import com.mint.db.RaftServiceGrpc;
import com.mint.db.util.LogUtil;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class InternalGrpcClient implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(InternalGrpcClient.class);
    private final ManagedChannel channel;
    private final RaftServiceGrpc.RaftServiceBlockingStub blockingStub;

    public InternalGrpcClient(String url) {
        channel = Grpc.newChannelBuilder(url, InsecureChannelCredentials.create())
                .build();
        blockingStub = RaftServiceGrpc.newBlockingStub(channel);
    }

    public Raft.VoteResponse requestVote(Raft.VoteRequest voteRequest) {
        logger.debug("RequestVote request {}", LogUtil.protobufMessageToString(voteRequest));

        try {
            return blockingStub.requestVote(voteRequest);
        } catch (StatusRuntimeException e) {
            logger.warn("RequestVote request failed", e);
            return null;
        }
    }

    public Raft.AppendEntriesResponse appendEntries(Raft.AppendEntriesRequest appendEntriesRequest) {
        logger.debug("AppendEntries request {}", LogUtil.protobufMessageToString(appendEntriesRequest));

        try {
            return blockingStub.appendEntries(appendEntriesRequest);
        } catch (StatusRuntimeException e) {
            logger.warn("AppendEntries request failed", e);
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }
}
