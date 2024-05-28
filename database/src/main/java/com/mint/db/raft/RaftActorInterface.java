package com.mint.db.raft;

import com.mint.db.Raft;
import com.mint.db.raft.model.Command;
import com.mint.db.raft.model.CommandResult;

public interface RaftActorInterface {
    /**
     * Called when timeout passed after [Environment.startTimeout]
     */
    void onTimeout();

    /**
     * Called on arriving [appendEntriesRequest] from another process {@link Raft.AppendEntriesRequest#getLeaderId()}
     */
    void onAppendEntry(Raft.AppendEntriesRequest appendEntriesRequest);

    /**
     * Called on arriving [appendEntriesResponse] from another process {@code srcId}
     *
     * @param srcId                 another process id
     * @param appendEntriesResponse append entry response
     */
    void onAppendEntryResult(int srcId, Raft.AppendEntriesResponse appendEntriesResponse);

    /**
     * Called on arriving [voteRequest] from another process {@link Raft.VoteRequest#getCandidateId()}
     */
    void onRequestVote(Raft.VoteRequest voteRequest);

    /**
     * Called on arriving [voteResponse] from another process {@code srcId}
     *
     * @param srcId        another process id
     * @param voteResponse vote response
     */
    void onRequestVoteResult(int srcId, Raft.VoteResponse voteResponse);

    /**
     * Called on client-requested command. The {@link Command#processId()} is always equal to the
     * identifier of this process
     */
    void onClientCommand(Command command);

    /**
     * Called on result of client command processing. The {@link Command#processId()} is always equal to the
     * identifier of this process
     */
    void onClientCommandResult(CommandResult commandResult);
}
