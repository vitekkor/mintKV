package com.mint.db.grpc.server;

import com.google.inject.Inject;
import com.mint.db.Raft;
import com.mint.db.RaftServiceGrpc;
import com.mint.db.raft.RaftActor;
import com.mint.db.raft.RaftActorInterface;
import io.grpc.stub.StreamObserver;

public class InternalServiceImpl extends RaftServiceGrpc.RaftServiceImplBase {
    private final RaftActorInterface raftActor;

    @Inject
    public InternalServiceImpl(RaftActorInterface raftActor) {
        this.raftActor = raftActor;
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
}
