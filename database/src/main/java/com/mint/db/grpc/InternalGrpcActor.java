package com.mint.db.grpc;

import com.mint.db.Raft;
import com.mint.db.grpc.client.InternalGrpcClient;

import java.util.Map;

public class InternalGrpcActor {
    private final Map<String, InternalGrpcClient> internalGrpcClients;

    public InternalGrpcActor(Map<String, InternalGrpcClient> internalGrpcClients) {
        this.internalGrpcClients = internalGrpcClients;
    }

    public void onLeaderCandidate(Raft.VoteRequest voteRequest) {
        // TODO send vote request to cluster nodes
    }

    public void onAppendEntityRequest(Raft.AppendEntriesRequest appendEntriesRequest) {
        // TODO send appendEntriesRequest to cluster nodes
    }
}
