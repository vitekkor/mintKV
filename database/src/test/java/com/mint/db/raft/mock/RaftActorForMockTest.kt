package com.mint.db.raft.mock

import com.mint.db.Raft
import com.mint.db.grpc.ExternalGrpcActorInterface
import com.mint.db.grpc.InternalGrpcActorInterface
import com.mint.db.raft.Environment
import com.mint.db.raft.RaftActor
import com.mint.db.raft.RaftActorInterface
import com.mint.db.raft.Timeout
import com.mint.db.raft.model.Command
import com.mint.db.raft.model.CommandResult
import java.lang.foreign.MemorySegment
import java.util.function.Consumer

class RaftActorForMockTest(
    private val actionSink: ActionSink,
    internalGrpcActor: InternalGrpcActorInterface,
    environment: Environment<MemorySegment>,
    externalGrpcActorInterface: ExternalGrpcActorInterface
) : RaftActorInterface {
    private val delegate = object : RaftActor(internalGrpcActor, environment, externalGrpcActorInterface) {
        override fun startTimeout(timeout: Timeout) {
            actionSink.removeActionIf { it is ProcessAction.StartTimeout }
            actionSink += ProcessAction.StartTimeout(timeout)
        }
    }

    override fun onTimeout() {
        delegate.onTimeout()
    }

    override fun onAppendEntry(
        appendEntriesRequest: Raft.AppendEntriesRequest?,
        onAppendEntriesResponse: Consumer<Raft.AppendEntriesResponse>?
    ) {
        delegate.onAppendEntry(appendEntriesRequest, onAppendEntriesResponse)
    }

    override fun onAppendEntryResult(srcId: Int, appendEntriesResponse: Raft.AppendEntriesResponse?) {
        delegate.onAppendEntryResult(srcId, appendEntriesResponse)
    }

    override fun onRequestVote(voteRequest: Raft.VoteRequest?, onVoteResponse: Consumer<Raft.VoteResponse>?) {
        delegate.onRequestVote(voteRequest, onVoteResponse)
    }

    override fun onRequestVoteResult(srcId: Int, voteResponse: Raft.VoteResponse?) {
        delegate.onRequestVoteResult(srcId, voteResponse)
    }

    override fun onClientCommand(command: Command?) {
        delegate.onClientCommand(command)
    }

    override fun onClientCommandResult(srcId: Int, command: Command?, commandResult: CommandResult?) {
        delegate.onClientCommandResult(srcId, command, commandResult)
    }
}
