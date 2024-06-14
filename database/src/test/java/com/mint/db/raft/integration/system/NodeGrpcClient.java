package com.mint.db.raft.integration.system;

import com.mint.DatabaseServiceGrpc;
import com.mint.DatabaseServiceOuterClass;
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

public class NodeGrpcClient implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(NodeGrpcClient.class);
    public static final int N_THREADS = 5;

    private final ExecutorService executor;
    private final ManagedChannel channel;
    private final DatabaseServiceGrpc.DatabaseServiceStub stub;

    public NodeGrpcClient(String url) {
        this.executor = Executors.newFixedThreadPool(N_THREADS);
        this.channel = Grpc.newChannelBuilder(url, InsecureChannelCredentials.create())
                .executor(executor)
                .build();

        this.stub = DatabaseServiceGrpc.newStub(channel);
    }

    public void get(
            DatabaseServiceOuterClass.GetRequest getRequest,
            TsiFrameProtector.Consumer<DatabaseServiceOuterClass.GetResponse> onGetResult
    ) {
        logger.debug("Get request {}", LogUtil.protobufMessageToString(getRequest));
        stub.get(getRequest, new StreamObserver<>() {
            @Override
            public void onNext(DatabaseServiceOuterClass.GetResponse response) {
                onGetResult.accept(response);
            }

            @Override
            public void onError(Throwable t) {
                logger.warn("Get request failed", t);
            }

            @Override
            public void onCompleted() {
                logger.debug("Get request completed");
            }
        });
    }

    public void insert(
            DatabaseServiceOuterClass.InsertRequest insertRequest,
            TsiFrameProtector.Consumer<DatabaseServiceOuterClass.SuccessfulWriteResponse> onInsertResult
    ) {
        logger.debug("Insert request {}", LogUtil.protobufMessageToString(insertRequest));
        stub.insert(insertRequest, new StreamObserver<>() {
            @Override
            public void onNext(DatabaseServiceOuterClass.SuccessfulWriteResponse response) {
                onInsertResult.accept(response);
            }

            @Override
            public void onError(Throwable t) {
                logger.warn("Insert request failed", t);
            }

            @Override
            public void onCompleted() {
                logger.debug("Insert request completed");
            }
        });
    }

    public void delete(
            DatabaseServiceOuterClass.DeleteRequest deleteRequest,
            TsiFrameProtector.Consumer<DatabaseServiceOuterClass.SuccessfulWriteResponse> onDeleteResult
    ) {
        logger.debug("Delete request {}", LogUtil.protobufMessageToString(deleteRequest));
        stub.delete(deleteRequest, new StreamObserver<>() {
            @Override
            public void onNext(DatabaseServiceOuterClass.SuccessfulWriteResponse response) {
                onDeleteResult.accept(response);
            }

            @Override
            public void onError(Throwable t) {
                logger.warn("Delete request failed", t);
            }

            @Override
            public void onCompleted() {
                logger.debug("Delete request completed");
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
