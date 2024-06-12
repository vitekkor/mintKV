package com.mint.db.grpc;

import com.mint.db.Raft;
import com.mint.db.raft.model.Command;
import com.mint.db.raft.model.CommandResult;
import com.mint.db.util.ClientCommandResultConsumer;
import io.grpc.stub.StreamObserver;

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
     * Sends the {@code appendEntriesRequest} message to the process {@code destId} (from 0 to [nProcesses - 1]).
     * Calls {@code onAppendEntryResult} on result.
     *
     * @param onAppendEntryResult int srcId, Raft.AppendEntriesResponse appendEntriesResponse
     */
    void sendAppendEntriesRequest(
            int destId,
            Raft.AppendEntriesRequest appendEntriesRequest,
            BiConsumer<Integer, Raft.AppendEntriesResponse> onAppendEntryResult
    );

    /**
     * Sends the {@code command} message to the process {@code destId} (from 0 to [nProcesses - 1]).
     * Calls {@code onCommandResult} on result.
     *
     * @param onCommandResult int srcId, CommandResult onCommandResult
     */
    void sendClientCommand(
            int destId,
            Command command,
            ClientCommandResultConsumer onCommandResult
    );


    /**
     * Adds mapping of {@code responseObserver} and command.
     *
     * @param command client command
     */


    void addClientCommandCallback(Command command, StreamObserver<?> responseObserver);


    /**
     * Sends the {@code commandResult} to the cluster node.
     *
     * @param commandResult result of applying client command
     */

    void onClientCommandResult(Command command, CommandResult commandResult);
}
