package com.mint.db.grpc.client;

import com.mint.DatabaseServiceGrpc;
import com.mint.DatabaseServiceOuterClass;
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

public class ExternalGrpcClient implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(ExternalGrpcClient.class);
    private final ManagedChannel channel;
    private final DatabaseServiceGrpc.DatabaseServiceBlockingStub blockingStub;

    public ExternalGrpcClient(String url) {
        channel = Grpc.newChannelBuilder(url, InsecureChannelCredentials.create())
                .build();
        // TODO use async stub
        blockingStub = DatabaseServiceGrpc.newBlockingStub(channel);
    }

    public DatabaseServiceOuterClass.GetResponse get(
            DatabaseServiceOuterClass.GetRequest getRequest
    ) {
        logger.debug("Get request {}", LogUtil.protobufMessageToString(getRequest));

        try {
            return blockingStub.get(getRequest);
        } catch (StatusRuntimeException e) {
            logger.warn("Get request failed", e);
            return null;
        }
    }

    public DatabaseServiceOuterClass.SuccessfulWriteResponse insert(
            DatabaseServiceOuterClass.InsertRequest insertRequest
    ) {
        logger.debug("Insert request {}", LogUtil.protobufMessageToString(insertRequest));

        try {
            return blockingStub.insert(insertRequest);
        } catch (StatusRuntimeException e) {
            logger.warn("Insert request failed", e);
            return null;
        }
    }

    public DatabaseServiceOuterClass.SuccessfulWriteResponse delete(
            DatabaseServiceOuterClass.DeleteRequest deleteRequest
    ) {
        logger.debug("Delete request {}", LogUtil.protobufMessageToString(deleteRequest));

        try {
            return blockingStub.delete(deleteRequest);
        } catch (StatusRuntimeException e) {
            logger.warn("Delete request failed", e);
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
