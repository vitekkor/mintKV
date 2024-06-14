package com.mint.db.grpc.server;

import com.google.inject.Inject;
import com.mint.db.Raft;
import com.mint.db.RaftServiceGrpc;
import com.mint.db.grpc.InternalGrpcActorInterface;
import com.mint.db.raft.RaftActorInterface;
import com.mint.db.raft.model.Command;
import com.mint.db.raft.model.GetCommand;
import com.mint.db.raft.model.InsertCommand;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InternalServiceImpl extends RaftServiceGrpc.RaftServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(InternalServiceImpl.class);
    private final RaftActorInterface raftActor;
    private final InternalGrpcActorInterface internalGrpcActor;

    @Inject
    public InternalServiceImpl(RaftActorInterface raftActor, InternalGrpcActorInterface internalGrpcActor) {
        this.raftActor = raftActor;
        this.internalGrpcActor = internalGrpcActor;
    }

    @Override
    public void requestVote(
            Raft.VoteRequest request,
            StreamObserver<Raft.VoteResponse> responseObserver
    ) {
        try {
            raftActor.onRequestVote(request, responseObserver::onNext);
        } finally {
            responseObserver.onCompleted();
        }
    }

    @Override
    public void appendEntries(
            Raft.AppendEntriesRequest request,
            StreamObserver<Raft.AppendEntriesResponse> responseObserver
    ) {
        try {
            raftActor.onAppendEntry(request, responseObserver::onNext);
        } finally {
            responseObserver.onCompleted();
        }
    }

    @Override
    public void clientCommand(
            Raft.ClientCommandRequestRPC request,
            StreamObserver<Raft.ClientCommandResponseRPC> responseObserver
    ) {
        var command = clientCommandRequestRPCToCommand(request);
        internalGrpcActor.addClientCommandCallback(command, responseObserver);
        raftActor.onClientCommand(command);
        //onCompleted will be called by the callback mechanism when the command is fully processed
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
