package com.mint.db.grpc.server;

import com.mint.DatabaseServiceGrpc;
import com.mint.DatabaseServiceOuterClass;
import com.mint.db.grpc.ExternalGrpcActorInterface;
import com.mint.db.grpc.client.ExternalGrpcClient;
import com.mint.db.raft.model.Command;
import com.mint.db.raft.model.CommandResult;
import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.function.BiConsumer;

public class ExternalServiceImpl
        extends DatabaseServiceGrpc.DatabaseServiceImplBase
        implements ExternalGrpcActorInterface {
    // TODO
    //     private final Map<Command, StreamObserver<?>> commandStreamObserverMap;
    private final Map<String, ExternalGrpcClient> externalGrpcClients;

    public ExternalServiceImpl(Map<String, ExternalGrpcClient> externalGrpcClients) {
        this.externalGrpcClients = externalGrpcClients;
    }

    @Override
    public void insert(
            DatabaseServiceOuterClass.InsertRequest request,
            StreamObserver<DatabaseServiceOuterClass.SuccessfulWriteResponse> responseObserver
    ) {
        // TODO convert to command and call RaftActorInterface::onClientCommand
        //      save command and observer to map
        super.insert(request, responseObserver);
    }

    @Override
    public void delete(
            DatabaseServiceOuterClass.DeleteRequest request,
            StreamObserver<DatabaseServiceOuterClass.SuccessfulWriteResponse> responseObserver
    ) {
        // TODO convert to command and call RaftActorInterface::onClientCommand
        //      save command and observer to map
        super.delete(request, responseObserver);
    }

    @Override
    public void get(
            DatabaseServiceOuterClass.GetRequest request,
            StreamObserver<DatabaseServiceOuterClass.GetResponse> responseObserver
    ) {
        // TODO convert to command and call RaftActorInterface::onClientCommand
        //      save command and observer to map
        super.get(request, responseObserver);
    }

    @Override
    public void sendClientCommand(int destId, Command command, BiConsumer<Command, CommandResult> onCommandResult) {
        // TODO send Command to destId async and call onCommandResult on response
    }

    @Override
    public void onClientCommandResult(Command command, CommandResult commandResult) {
        // TODO get responseObserver from map
        //      convert command result and responseObserver::next
    }
}
