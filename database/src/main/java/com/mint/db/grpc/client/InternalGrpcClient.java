package com.mint.db.grpc.client;

import com.mint.db.Raft;
import com.mint.db.RaftServiceGrpc;
import com.mint.db.util.LogUtil;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.alts.internal.TsiFrameProtector;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class InternalGrpcClient implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(InternalGrpcClient.class);
    public static final int N_THREADS = 5;

    private final ExecutorService executor;
    private final ManagedChannel channel;
    private final RaftServiceGrpc.RaftServiceStub stub;

    public InternalGrpcClient(String url) {
        executor = Executors.newFixedThreadPool(N_THREADS);
        channel = Grpc.newChannelBuilder(url, InsecureChannelCredentials.create())
                .executor(executor)
                .build();

        stub = RaftServiceGrpc.newStub(channel);
    }

    public void requestVote(Raft.VoteRequest voteRequest, TsiFrameProtector.Consumer<Raft.VoteResponse> onRequestVoteResult) {
        logger.debug("RequestVote request {}", LogUtil.protobufMessageToString(voteRequest));
        stub.requestVote(voteRequest, new StreamObserver<>() {
            @Override
            public void onNext(Raft.VoteResponse response) {
                onRequestVoteResult.accept(response);
            }

            @Override
            public void onError(Throwable t) {
                logger.warn("RequestVote request failed", t);
            }

            @Override
            public void onCompleted() {
                logger.debug("RequestVote completed");
            }
        });
    }

    public void appendEntries(Raft.AppendEntriesRequest appendEntriesRequest, TsiFrameProtector.Consumer<Raft.AppendEntriesResponse> onAppendEntriesResult) {
        logger.debug("AppendEntries request {}", LogUtil.protobufMessageToString(appendEntriesRequest));
        stub.appendEntries(appendEntriesRequest, new StreamObserver<>() {
            @Override
            public void onNext(Raft.AppendEntriesResponse response) {
                onAppendEntriesResult.accept(response);
            }

            @Override
            public void onError(Throwable t) {
                logger.warn("AppendEntries request failed", t);
            }

            @Override
            public void onCompleted() {
                logger.debug("AppendEntries completed");
            }
        });
    }

    @Override
    public void close() throws IOException {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            executor.shutdown();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }
}
