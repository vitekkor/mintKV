package com.mint.db.http.server;

import com.google.inject.Inject;
import com.mint.db.config.NodeConfig;
import com.mint.db.config.annotations.CallbackKeeperBean;
import com.mint.db.config.annotations.NodeConfiguration;
import com.mint.db.config.annotations.RaftActorBean;
import com.mint.db.http.server.dto.DeleteRequestDto;
import com.mint.db.http.server.dto.GetRequestDto;
import com.mint.db.http.server.dto.InsertRequestDto;
import com.mint.db.raft.RaftActor;
import com.mint.db.raft.model.*;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExternalHttpServer {
    private static final Logger log = LoggerFactory.getLogger(ExternalHttpServer.class);
    private final HttpServer server;
    private final NodeConfig nodeConfig;
    private final RaftActor raftActor;
    private final CallbackKeeper callbackKeeper;

    @Inject
    public ExternalHttpServer(
            @NodeConfiguration NodeConfig config,
            @RaftActorBean RaftActor raftActor,
            @CallbackKeeperBean CallbackKeeper callbackKeeper
    ) {
        this.nodeConfig = config;
        this.raftActor = raftActor;
        this.callbackKeeper = callbackKeeper;
        try {
            this.server = HttpServer.create(new InetSocketAddress(config.getHttpPort()), 0);
            ExecutorService executor = Executors.newFixedThreadPool(10);
            server.setExecutor(executor);
            server.createContext("/insert", this::handleInsert);
            server.createContext("/delete", this::handleDelete);
            server.createContext("/get", this::handleGet);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create HTTP server", e);
        }
    }

    public void onClientCommandResult(Command command, CommandResult commandResult, HttpExchange exchange) {
        try {
            if (commandResult instanceof GetCommandResult) {
                String response;
                if (commandResult.value() != null) {
                    response = "Found: " + commandResult.value();
                    exchange.sendResponseHeaders(200, response.length());
                } else {
                    response = "Not Found";
                    exchange.sendResponseHeaders(404, response.length());
                }
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes());
                os.close();
            } else if (commandResult instanceof InsertCommandResult) {
                String response = "Insert Successful";
                exchange.sendResponseHeaders(200, response.length());
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes());
                os.close();
            }
        } catch (IOException e) {
            log.error("Failed to send response", e);
        }
        log.debug("Command result processed for command: {}", command);
    }

    private void handleInsert(HttpExchange exchange) throws IOException {
        InsertRequestDto request = InsertRequestDto.valueOf(exchange);
        if (request == null) {
            try {
                exchange.sendResponseHeaders(400, 0);
            } catch (IOException e) {
                log.error("Failed to send response", e);
            }
            return;
        }
        Command command = converInsertRequestDtoToInsertCommand(request);
        addClientCommandCallback(command, exchange);
        raftActor.onClientCommand(command);
    }

    private void handleGet(HttpExchange exchange) throws IOException {

        GetRequestDto request = GetRequestDto.valueOf(exchange);
        if (request == null) {
            try {
                exchange.sendResponseHeaders(400, 0);
            } catch (IOException e) {
                log.error("Failed to send response", e);
            }
            return;
        }
        Command command = converGetRequestDtoToGetCommand(request);
        addClientCommandCallback(command, exchange);
        raftActor.onClientCommand(command);
    }

    private void handleDelete(HttpExchange exchange) throws IOException {
        DeleteRequestDto request = DeleteRequestDto.valueOf(exchange);
        if (request == null) {
            try {
                exchange.sendResponseHeaders(400, 0);
            } catch (IOException e) {
                log.error("Failed to send response", e);
            }
            return;
        }
        Command command = converDeleteRequestDtoToDeleteCommand(request);
        addClientCommandCallback(command, exchange);
        raftActor.onClientCommand(command);
    }

    private Command converInsertRequestDtoToInsertCommand(InsertRequestDto request) {
        return new InsertCommand(
                nodeConfig.getNodeId(),
                request.key(),
                request.value(),
                request.uncommitted()
        );
    }

    private Command converGetRequestDtoToGetCommand(GetRequestDto request) {
        return new GetCommand(
                nodeConfig.getNodeId(),
                request.key(),
                request.readMode()
        );
    }

    private Command converDeleteRequestDtoToDeleteCommand(DeleteRequestDto request) {
        return new InsertCommand(
                nodeConfig.getNodeId(),
                request.key(),
                null,
                request.uncommitted()
        );
    }

    public void start() {
        server.start();
    }

    private void stop() {
        server.stop(0);
    }

    private void addClientCommandCallback(Command command, HttpExchange exchange) {
        callbackKeeper.addClientCommandCallback(command, (c, r) -> {
            onClientCommandResult(c, r, exchange);
        });
    }
}
