package com.mint.db.grpc;

import com.google.protobuf.ByteString;
import com.mint.db.Raft;
import com.mint.db.grpc.client.InternalGrpcClient;
import com.mint.db.raft.model.Command;
import com.mint.db.raft.model.CommandResult;
import com.mint.db.raft.model.GetCommand;
import com.mint.db.raft.model.GetCommandResult;
import com.mint.db.raft.model.InsertCommand;
import com.mint.db.raft.model.InsertCommandResult;
import com.mint.db.util.ClientCommandResultConsumer;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import static com.mint.db.Raft.Operation.DELETE;
import static com.mint.db.Raft.Operation.GET;
import static com.mint.db.Raft.Operation.PUT;

public class InternalGrpcActor implements InternalGrpcActorInterface {
    private static final Logger logger = LoggerFactory.getLogger(InternalGrpcActor.class);

    private final Map<Integer, InternalGrpcClient> internalGrpcClients;
    private final Map<Command, StreamObserver<?>> commandStreamObserverMap = new ConcurrentHashMap<>();

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
    public void sendClientCommand(int destId, Command command, ClientCommandResultConsumer onCommandResult) {
        InternalGrpcClient client = internalGrpcClients.get(destId);
        if (client == null) {
            logger.error("No client found for destId: {}", destId);
        } else {
            logger.debug("Sending command {} to destId {}", command, destId);
            if (command instanceof InsertCommand insertCommand) {
                if (insertCommand.value() != null) {
                    sendInsertCommand(destId, client, insertCommand, onCommandResult);
                } else {
                    sendDeleteCommand(destId, client, insertCommand, onCommandResult);
                }
            } else if (command instanceof GetCommand getCommand) {
                sendGetCommand(destId, client, getCommand, onCommandResult);
            }
        }
    }

    private void sendGetCommand(
            int destId,
            InternalGrpcClient client,
            GetCommand command,
            ClientCommandResultConsumer onCommandResult
    ) {
        Raft.ClientCommandRequestRPC commandRequestRPC = Raft.ClientCommandRequestRPC.newBuilder()
                .setOperation(GET)
                .setKey(ByteString.copyFromUtf8(command.key()))
                .setReadMode(command.readMode())
                .setProcessId(command.processId())
                .build();
        client.get(commandRequestRPC, response -> {
            logger.debug("Get command result received for key: {}", command.key());
            String value = response.getValue() != com.google.protobuf.ByteString.EMPTY
                    ? response.getValue().toStringUtf8()
                    : null;
            GetCommandResult result = new GetCommandResult(
                    response.getTerm(),
                    command.key(),
                    value
            );
            onCommandResult.accept(destId, command, result);
        });
    }

    private void sendInsertCommand(
            int destId,
            InternalGrpcClient client,
            InsertCommand command,
            ClientCommandResultConsumer onCommandResult
    ) {
        Raft.ClientCommandRequestRPC commandRequestRPC = Raft.ClientCommandRequestRPC.newBuilder()
                .setOperation(PUT)
                .setKey(ByteString.copyFromUtf8(command.key()))
                .setValue(ByteString.copyFromUtf8(command.value()))
                .setUncommitted(command.uncommitted())
                .setProcessId(command.processId())
                .build();
        client.insert(commandRequestRPC, response -> {
            logger.debug("Insert command result received for key: {}", command.key());
            InsertCommandResult result = new InsertCommandResult(response.getTerm(), command.key());
            onCommandResult.accept(destId, command, result);
        });
    }

    private void sendDeleteCommand(
            int destId,
            InternalGrpcClient client,
            InsertCommand command,
            ClientCommandResultConsumer onCommandResult
    ) {
        Raft.ClientCommandRequestRPC commandRequestRPC = Raft.ClientCommandRequestRPC.newBuilder()
                .setOperation(DELETE)
                .setKey(ByteString.copyFromUtf8(command.key()))
                .setUncommitted(command.uncommitted())
                .setProcessId(command.processId())
                .build();
        client.delete(commandRequestRPC, response -> {
            logger.debug("Insert command result received for key: {}", command.key());
            InsertCommandResult result = new InsertCommandResult(response.getTerm(), command.key());
            onCommandResult.accept(destId, command, result);
        });
    }

    @Override
    public void addClientCommandCallback(Command command, StreamObserver<?> responseObserver) {
        commandStreamObserverMap.put(command, responseObserver);
    }

    @Override
    public void onClientCommandResult(Command command, CommandResult commandResult) {
        StreamObserver<?> responseObserver = commandStreamObserverMap.remove(command);
        if (responseObserver == null) {
            logger.warn("No response observer found for command: {}", command);
            return;
        }
        ByteString value = commandResult.value() != null
                ? ByteString.copyFromUtf8(commandResult.value())
                : ByteString.EMPTY;
        Raft.ClientCommandResponseRPC response = Raft.ClientCommandResponseRPC.newBuilder()
                .setTerm(commandResult.term())
                .setKey(ByteString.copyFromUtf8(commandResult.key()))
                .setValue(value)
                .build();
        ((StreamObserver<Raft.ClientCommandResponseRPC>) responseObserver).onNext(response);
        responseObserver.onCompleted();
        logger.debug("Command result processed for command: {}", command);
    }
}
