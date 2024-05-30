package com.mint.db.grpc;

import com.mint.db.Raft;
import com.mint.db.grpc.client.InternalGrpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.BiConsumer;

public class InternalGrpcActor implements InternalGrpcActorInterface {
    private static final Logger logger = LoggerFactory.getLogger(InternalGrpcClient.class);

    private final Map<String, InternalGrpcClient> internalGrpcClients;

    public InternalGrpcActor(Map<String, InternalGrpcClient> internalGrpcClients) {
        this.internalGrpcClients = internalGrpcClients;
    }

    @Override
    public void sendVoteRequest(
            Raft.VoteRequest voteRequest,
            BiConsumer<Integer, Raft.VoteResponse> onRequestVoteResult
    ) {
        for (Map.Entry<String, InternalGrpcClient> entry : internalGrpcClients.entrySet()) {
            int nodeId = Integer.parseInt(entry.getKey());
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
        for (Map.Entry<String, InternalGrpcClient> entry : internalGrpcClients.entrySet()) {
            int nodeId = Integer.parseInt(entry.getKey());
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
        String key = String.valueOf(destId);
        InternalGrpcClient client = internalGrpcClients.get(key);

        if (client != null) {
            client.appendEntries(appendEntriesRequest, response -> {
                logger.debug("AppendEntriesResponse from node {}: {}", destId, response);
                onAppendEntryResult.accept(destId, response);
            });
        } else {
            logger.warn("No client found for destId {}", destId);
        }
    }
}
