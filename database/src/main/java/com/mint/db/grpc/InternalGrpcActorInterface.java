package com.mint.db.grpc;

import com.mint.db.Raft;

import java.util.function.BiConsumer;

public interface InternalGrpcActorInterface {
    /**
     * Sends the voteRequest message to all processes in cluster. Calls {@code onRequestVoteResult} on result.
     *
     * @param onRequestVoteResult int srcId, Raft.VoteResponse voteResponse
     */
    void sendVoteRequest(Raft.VoteRequest voteRequest, BiConsumer<Integer, Raft.VoteResponse> onRequestVoteResult);

    /**
     * Sends the appendEntriesRequest message to all processes in cluster. Calls {@code onAppendEntryResult} on result.
     *
     * @param onAppendEntryResult int srcId, Raft.AppendEntriesResponse appendEntriesResponse
     */
    void sendAppendEntriesRequest(
            Raft.AppendEntriesRequest appendEntriesRequest,
            BiConsumer<Integer, Raft.AppendEntriesResponse> onAppendEntryResult
    );

    /**
     * Sends the {@code appendEntriesRequest} message to the process {@code destId} (from 1 to [nProcesses]).
     * Calls {@code onAppendEntryResult} on result.
     *
     * @param onAppendEntryResult int srcId, Raft.AppendEntriesResponse appendEntriesResponse
     */
    void sendAppendEntriesRequest(
            int destId,
            Raft.AppendEntriesRequest appendEntriesRequest,
            BiConsumer<Integer, Raft.AppendEntriesResponse> onAppendEntryResult
    );
}
