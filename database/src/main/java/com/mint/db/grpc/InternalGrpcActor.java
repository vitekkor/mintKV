package com.mint.db.grpc;

import com.mint.db.Raft;

public class InternalGrpcActor {
    public void onLeaderCandidate(Raft.VoteRequest voteRequest) {
        // TODO send vote request to cluster nodes
    }

    public void onAppendEntityRequest(Raft.AppendEntriesRequest appendEntriesRequest) {
        // TODO send appendEntriesRequest to cluster nodes
    }
}
