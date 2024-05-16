package com.mint.db.grpc;

import com.mint.db.config.NodeConfig;
import com.mint.db.exceptions.ServerStartupException;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Server {
    private static final Logger log = LoggerFactory.getLogger(Server.class);
    private final io.grpc.Server server;

    public Server(NodeConfig config) {
        this.server = ServerBuilder
                .forPort(config.getPort())
                .addService(new ExternalServiceImpl())
                .addService(new InternalServiceImpl())
                .build();
    }

    public void start() {
        try {
            server.start();
        } catch (IOException e) {
            log.error("Failed to start server", e);
            throw new ServerStartupException();
        }
    }

    public void stop() {
        server.shutdown();
    }
}
