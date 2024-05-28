package com.mint.db.grpc.server;

import com.google.inject.Inject;
import com.mint.db.config.NodeConfig;
import com.mint.db.config.annotations.ExternalGrpcActorBean;
import com.mint.db.config.annotations.NodeConfiguration;
import com.mint.db.config.annotations.RaftActorBean;
import com.mint.db.exceptions.ServerStartupException;
import com.mint.db.raft.RaftActor;
import com.mint.db.raft.RaftActorInterface;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class Server {
    private static final Logger log = LoggerFactory.getLogger(Server.class);
    private final io.grpc.Server grpcServer;

    @Inject
    public Server(
            @NodeConfiguration NodeConfig config,
            @RaftActorBean RaftActorInterface raftActor,
            @ExternalGrpcActorBean ExternalServiceImpl externalService
    ) {
        this.grpcServer = ServerBuilder
                .forPort(config.getPort())
                .addService(externalService)
                .addService(new InternalServiceImpl(raftActor))
                .build();
    }

    public void start() {
        try {
            grpcServer.start();
            log.info("Server started, listening on {}", grpcServer.getPort());
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("*** shutting down gRPC server since JVM is shutting down");
                try {
                    Server.this.stop();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Interrupted", e);
                }
                log.info("*** server shut down");
            }));
        } catch (IOException e) {
            log.error("Failed to start server", e);
            throw new ServerStartupException();
        }
    }

    public void stop() throws InterruptedException {
        grpcServer.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (grpcServer != null) {
            grpcServer.awaitTermination();
        }
    }
}
