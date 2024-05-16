package com.mint.db;

import com.mint.db.config.NodeConfig;
import com.mint.db.grpc.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MintKVDatabase {
    private static final Logger log = LoggerFactory.getLogger(MintKVDatabase.class);

    public static void main(String[] args) {
        NodeConfig nodeConfig = new NodeConfig(
                8080,
                1,
                "logs/1",
                List.of("localhost:8080")
        );
        Server server = new Server(nodeConfig);
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down server");
            server.stop();
        }));

        while (true) {
        }
    }
}
