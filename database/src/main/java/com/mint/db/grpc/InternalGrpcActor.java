package com.mint.db.grpc;

import com.mint.db.Raft;
import com.mint.db.grpc.client.InternalGrpcClient;

import java.util.Map;
import java.util.function.BiConsumer;

public class InternalGrpcActor implements InternalGrpcActorInterface {
    private final Map<String, InternalGrpcClient> internalGrpcClients;

    public InternalGrpcActor(Map<String, InternalGrpcClient> internalGrpcClients) {
        this.internalGrpcClients = internalGrpcClients;
    }

    @Override
    public void sendVoteRequest(
            Raft.VoteRequest voteRequest,
            BiConsumer<Integer, Raft.VoteResponse> onRequestVoteResult
    ) {
        // TODO send vote request to cluster nodes
    }

    @Override
    public void sendAppendEntriesRequest(
            Raft.AppendEntriesRequest appendEntriesRequest,
            BiConsumer<Integer, Raft.AppendEntriesResponse> onAppendEntryResult
    ) {
        // TODO send vote request to cluster nodes
    }

    @Override
    public void sendAppendEntriesRequest(
            int destId, Raft.AppendEntriesRequest appendEntriesRequest,
            BiConsumer<Integer, Raft.AppendEntriesResponse> onAppendEntryResult
    ) {
        // TODO send vote request to destId node
    }
}
