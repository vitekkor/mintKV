package com.mint.db.raft;

import com.mint.db.Raft;
import com.mint.db.raft.model.Command;
import com.mint.db.raft.model.CommandResult;

import java.util.function.Consumer;

public interface RaftActorInterface {
    /**
     * Called when timeout passed after {@link RaftActor#startTimeout(Timeout)}.
     */
    void onTimeout();

    /**
     * Called on arriving [appendEntriesRequest] from another process {@link Raft.AppendEntriesRequest#getLeaderId()}.
     */
    void onAppendEntry(
            Raft.AppendEntriesRequest appendEntriesRequest,
            Consumer<Raft.AppendEntriesResponse> onVoteResponse
    );

    /**
     * Called on arriving [appendEntriesResponse] from another process {@code srcId}.
     *
     * @param srcId                 another process id
     * @param appendEntriesResponse append entry response
     */
    void onAppendEntryResult(int srcId, Raft.AppendEntriesResponse appendEntriesResponse);

    /**
     * Called on arriving [voteRequest] from another process {@link Raft.VoteRequest#getCandidateId()}.
     */
    void onRequestVote(Raft.VoteRequest voteRequest, Consumer<Raft.VoteResponse> onVoteResponse);

    /**
     * Called on arriving [voteResponse] from another process {@code srcId}.
     *
     * @param srcId        another process id
     * @param voteResponse vote response
     */
    void onRequestVoteResult(int srcId, Raft.VoteResponse voteResponse);

    /**
     * Called on client-requested command. The {@link Command#processId()} is always equal to the
     * identifier of this process.
     */
    void onClientCommand(Command command);

    /**
     * Called on result of client command processing. The {@link Command#processId()} is always equal to the
     * identifier of this process.
     */
    void onClientCommandResult(CommandResult commandResult);
}
