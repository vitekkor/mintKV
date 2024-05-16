package com.mint.db.grpc;

import com.mint.db.Raft;
import com.mint.db.RaftServiceGrpc;
import io.grpc.stub.StreamObserver;

public class InternalServiceImpl extends RaftServiceGrpc.RaftServiceImplBase {
    @Override
    public void requestVote(
            Raft.VoteRequest request,
            StreamObserver<Raft.VoteResponse> responseObserver
    ) {
        super.requestVote(request, responseObserver);
    }

    @Override
    public void appendEntries(
            Raft.AppendEntriesRequest request,
            StreamObserver<Raft.AppendEntriesResponse> responseObserver
    ) {
        super.appendEntries(request, responseObserver);
    }
}
