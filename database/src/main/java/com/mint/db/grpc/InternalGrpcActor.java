package com.mint.db.grpc;

import com.google.protobuf.ByteString;
import com.mint.db.Raft;
import com.mint.db.grpc.client.InternalGrpcClient;
import com.mint.db.raft.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.BiConsumer;

import static com.mint.db.Raft.Operation.*;

public class InternalGrpcActor implements InternalGrpcActorInterface {
    private static final Logger logger = LoggerFactory.getLogger(InternalGrpcClient.class);

    private final Map<Integer, InternalGrpcClient> internalGrpcClients;

    public InternalGrpcActor(Map<Integer, InternalGrpcClient> internalGrpcClients) {
        this.internalGrpcClients = internalGrpcClients;
    }

    @Override
    public void sendVoteRequest(
            Raft.VoteRequest voteRequest,
            BiConsumer<Integer, Raft.VoteResponse> onRequestVoteResult
    ) {
        for (Map.Entry<Integer, InternalGrpcClient> entry : internalGrpcClients.entrySet()) {
            int nodeId = entry.getKey();
            InternalGrpcClient client = entry.getValue();
            client.requestVote(voteRequest, response -> {
                logger.debug("VoteResponse from node {}: {}", nodeId, response);
                onRequestVoteResult.accept(nodeId, response);
            });
        }
    }

    @Override
    public void sendAppendEntriesRequest(
            Raft.AppendEntriesRequest appendEntriesRequest,
            BiConsumer<Integer, Raft.AppendEntriesResponse> onAppendEntryResult
    ) {
        for (Map.Entry<Integer, InternalGrpcClient> entry : internalGrpcClients.entrySet()) {
            int nodeId = entry.getKey();
            InternalGrpcClient client = entry.getValue();
            client.appendEntries(appendEntriesRequest, response -> {
                logger.debug("AppendEntriesResponse from node {}: {}", nodeId, response);
                onAppendEntryResult.accept(nodeId, response);
            });
        }
    }

    @Override
    public void sendAppendEntriesRequest(
            int destId,
            Raft.AppendEntriesRequest appendEntriesRequest,
            BiConsumer<Integer, Raft.AppendEntriesResponse> onAppendEntryResult
    ) {
        InternalGrpcClient client = internalGrpcClients.get(destId);

        if (client != null) {
            client.appendEntries(appendEntriesRequest, response -> {
                logger.debug("AppendEntriesResponse from node {}: {}", destId, response);
                onAppendEntryResult.accept(destId, response);
            });
        } else {
            logger.warn("No client found for destId {}", destId);
        }
    }

    /**
     * For redirect command if this node isn't leader.
     *
     * @param destId          leader node Id
     * @param command         command
     * @param onCommandResult onCommandResult
     */
    @Override
    public void sendClientCommand(int destId, Command command, BiConsumer<Command, CommandResult> onCommandResult) {
        InternalGrpcClient client = internalGrpcClients.get(destId);
        if (client == null) {
            logger.error("No client found for destId: " + destId);
        } else {
            logger.debug("Sending command {} to destId {}", command, destId);
            if (command instanceof InsertCommand insertCommand) {
                if (insertCommand.value() != null) {
                    sendInsertCommand(client, insertCommand, onCommandResult);
                } else {
                    sendDeleteCommand(client, insertCommand, onCommandResult);
                }
            } else if (command instanceof GetCommand getCommand) {
                sendGetCommand(client, getCommand, onCommandResult);
            }
        }
    }

    private void sendGetCommand(
            InternalGrpcClient client,
            GetCommand command,
            BiConsumer<Command, CommandResult> onCommandResult
    ) {
        Raft.ClientCommandRequestRPC commandRequestRPC = Raft.ClientCommandRequestRPC.newBuilder()
                .setOperation(GET)
                .setKey(ByteString.copyFromUtf8(command.key()))
                .setReadMode(command.readMode())
                .setProcessId((int) command.processId())
                .build();
        client.get(commandRequestRPC, response -> {
            logger.debug("Get command result received for key: {}", command.key());
            GetCommandResult result = new GetCommandResult(
                    response.getTerm(),
                    command.key(),
                    response.getValue().toStringUtf8()
            );
            onCommandResult.accept(command, result);
        });
    }

    private void sendInsertCommand(
            InternalGrpcClient client,
            InsertCommand command,
            BiConsumer<Command, CommandResult> onCommandResult
    ) {
        Raft.ClientCommandRequestRPC commandRequestRPC = Raft.ClientCommandRequestRPC.newBuilder()
                .setOperation(PUT)
                .setKey(ByteString.copyFromUtf8(command.key()))
                .setValue(ByteString.copyFromUtf8(command.value()))
                .setUncommitted(command.uncommitted())
                .setProcessId((int) command.processId())
                .build();
        client.insert(commandRequestRPC, response -> {
            logger.debug("Insert command result received for key: {}", command.key());
            InsertCommandResult result = new InsertCommandResult(response.getTerm(), command.key());
            onCommandResult.accept(command, result);
        });
    }

    private void sendDeleteCommand(
            InternalGrpcClient client,
            InsertCommand command,
            BiConsumer<Command, CommandResult> onCommandResult
    ) {
        Raft.ClientCommandRequestRPC commandRequestRPC = Raft.ClientCommandRequestRPC.newBuilder()
                .setOperation(DELETE)
                .setKey(ByteString.copyFromUtf8(command.key()))
                .setUncommitted(command.uncommitted())
                .setProcessId((int) command.processId())
                .build();
        client.delete(commandRequestRPC, response -> {
            logger.debug("Insert command result received for key: {}", command.key());
            InsertCommandResult result = new InsertCommandResult(response.getTerm(), command.key());
            onCommandResult.accept(command, result);
        });
    }
}
