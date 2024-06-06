package com.mint.db.grpc.server;

import com.google.inject.Inject;
import com.mint.db.Raft;
import com.mint.db.RaftServiceGrpc;
import com.mint.db.grpc.ExternalGrpcActorInterface;
import com.mint.db.raft.RaftActorInterface;
import com.mint.db.raft.model.Command;
import com.mint.db.raft.model.GetCommand;
import com.mint.db.raft.model.InsertCommand;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InternalServiceImpl extends RaftServiceGrpc.RaftServiceImplBase {
    private final RaftActorInterface raftActor;
    private final ExternalGrpcActorInterface externalGrpcActorInterface;
    private static final Logger logger = LoggerFactory.getLogger(ExternalServiceImpl.class);

    @Inject
    public InternalServiceImpl(RaftActorInterface raftActor, ExternalGrpcActorInterface externalGrpcActorInterface) {
        this.raftActor = raftActor;
        this.externalGrpcActorInterface = externalGrpcActorInterface;
    }

    @Override
    public void requestVote(
            Raft.VoteRequest request,
            StreamObserver<Raft.VoteResponse> responseObserver
    ) {
        raftActor.onRequestVote(request, (responseObserver::onNext));
    }

    @Override
    public void appendEntries(
            Raft.AppendEntriesRequest request,
            StreamObserver<Raft.AppendEntriesResponse> responseObserver
    ) {
        raftActor.onAppendEntry(request, responseObserver::onNext);
    }

    @Override
    public void clientCommand(
            Raft.ClientCommandRequestRPC request,
            StreamObserver<Raft.ClientCommandResponseRPC> responseObserver
    ) {
        var command = clientCommandRequestRPCToCommand(request);
        raftActor.onClientCommand(command);
        externalGrpcActorInterface.addClientCommandCallback(command, responseObserver);
    }

    private Command clientCommandRequestRPCToCommand(Raft.ClientCommandRequestRPC request) {
        logger.debug("Converting request to Command: {}", request);
        return switch (request.getOperation()) {
            case GET -> new GetCommand(
                    request.getProcessId(),
                    request.getKey().toStringUtf8(),
                    request.getReadMode()
            );
            case PUT -> new InsertCommand(
                    request.getProcessId(),
                    request.getKey().toStringUtf8(),
                    request.getValue().toStringUtf8(),
                    request.getUncommitted()
            );
            case DELETE -> new InsertCommand(
                    request.getProcessId(),
                    request.getKey().toStringUtf8(),
                    null,
                    request.getUncommitted()
            );
            case UNRECOGNIZED, UNDEFINED ->
                    throw new IllegalArgumentException(STR."Unrecognized operation type \{request.getOperation()}");
        };
    }
}
