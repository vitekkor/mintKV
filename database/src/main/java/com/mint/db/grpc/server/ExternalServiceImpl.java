package com.mint.db.grpc.server;

import com.google.protobuf.ByteString;
import com.mint.DatabaseServiceGrpc;
import com.mint.DatabaseServiceOuterClass;
import com.mint.db.config.NodeConfig;
import com.mint.db.config.annotations.CallbackKeeperBean;
import com.mint.db.config.annotations.NodeConfiguration;
import com.mint.db.config.annotations.RaftActorBean;
import com.mint.db.grpc.ExternalGrpcActorInterface;
import com.mint.db.http.server.CallbackKeeper;
import com.mint.db.raft.RaftActor;
import com.mint.db.raft.model.Command;
import com.mint.db.raft.model.CommandResult;
import com.mint.db.raft.model.GetCommand;
import com.mint.db.raft.model.GetCommandResult;
import com.mint.db.raft.model.InsertCommand;
import com.mint.db.raft.model.InsertCommandResult;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExternalServiceImpl
        extends DatabaseServiceGrpc.DatabaseServiceImplBase
        implements ExternalGrpcActorInterface {

    private static final Logger logger = LoggerFactory.getLogger(ExternalServiceImpl.class);
    private final RaftActor raftActor;
    private final NodeConfig nodeConfig;
    private final CallbackKeeper callbackKeeper;

    public ExternalServiceImpl(
            @NodeConfiguration NodeConfig nodeConfig,
            @RaftActorBean RaftActor raftActor,
            @CallbackKeeperBean CallbackKeeper callbackKeeper
    ) {
        this.nodeConfig = nodeConfig;
        this.raftActor = raftActor;
        this.callbackKeeper = callbackKeeper;
    }

    @Override
    public void insert(
            DatabaseServiceOuterClass.InsertRequest request,
            StreamObserver<DatabaseServiceOuterClass.SuccessfulWriteResponse> responseObserver
    ) {
        logger.debug("Received insert request: {}", request);
        Command command = convertInsertRequestToCommand(request);
        addClientCommandCallback(command, responseObserver);
        raftActor.onClientCommand(command);
    }

    @Override
    public void delete(
            DatabaseServiceOuterClass.DeleteRequest request,
            StreamObserver<DatabaseServiceOuterClass.SuccessfulWriteResponse> responseObserver
    ) {
        logger.debug("Received delete request: {}", request);
        Command command = convertDeleteRequestToCommand(request);
        addClientCommandCallback(command, responseObserver);
        raftActor.onClientCommand(command);
    }

    @Override
    public void get(
            DatabaseServiceOuterClass.GetRequest request,
            StreamObserver<DatabaseServiceOuterClass.GetResponse> responseObserver
    ) {
        logger.debug("Received get request: {}", request);
        Command command = convertGetRequestToCommand(request);
        addClientCommandCallback(command, responseObserver);
        raftActor.onClientCommand(command);
    }

    @Override
    public void onClientCommandResult(Command command, CommandResult commandResult, StreamObserver<?> responseObserver) {
        if (responseObserver == null) {
            logger.warn("No response observer found for command: {}", command);
            return;
        }
        if (commandResult instanceof GetCommandResult) {
            DatabaseServiceOuterClass.GetResponse response;
            if (commandResult.value() != null) {
                response = DatabaseServiceOuterClass.GetResponse.newBuilder()
                        .setValue(ByteString.copyFromUtf8(commandResult.value()))
                        .setFound(true)
                        .build();
            } else {
                response = DatabaseServiceOuterClass.GetResponse.newBuilder()
                        .setFound(false)
                        .build();
            }
            ((StreamObserver<DatabaseServiceOuterClass.GetResponse>) responseObserver).onNext(response);
            responseObserver.onCompleted();
        } else if (commandResult instanceof InsertCommandResult) {
            DatabaseServiceOuterClass.SuccessfulWriteResponse response =
                    DatabaseServiceOuterClass.SuccessfulWriteResponse.newBuilder().build();
            ((StreamObserver<DatabaseServiceOuterClass.SuccessfulWriteResponse>) responseObserver).onNext(response);
            responseObserver.onCompleted();
        }
        logger.debug("Command result processed for command: {}", command);
    }

    private Command convertInsertRequestToCommand(DatabaseServiceOuterClass.InsertRequest request) {
        logger.debug("Converting InsertRequest to Command: {}", request);
        return new InsertCommand(
                nodeConfig.getNodeId(),
                request.getKey(),
                request.getValue(),
                request.getUncommitted()
        );
    }

    private Command convertDeleteRequestToCommand(DatabaseServiceOuterClass.DeleteRequest request) {
        logger.debug("Converting DeleteRequest to Command: {}", request);
        return new InsertCommand(
                nodeConfig.getNodeId(),
                request.getKey(),
                null,
                request.getUncommitted()
        );
    }

    private Command convertGetRequestToCommand(DatabaseServiceOuterClass.GetRequest request) {
        logger.debug("Converting GetRequest to Command: {}", request);
        return new GetCommand(
                nodeConfig.getNodeId(),
                request.getKey().toStringUtf8(),
                request.getMode()
        );
    }

    private void addClientCommandCallback(Command command, StreamObserver<?> responseObserver) {
        callbackKeeper.addClientCommandCallback(command, (c, r) -> {
            onClientCommandResult(c, r, responseObserver);
        });
    }
}
