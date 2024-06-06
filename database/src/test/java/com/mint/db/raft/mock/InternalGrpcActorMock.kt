package com.mint.db.raft.mock

import com.google.protobuf.ByteString
import com.mint.db.Raft
import com.mint.db.grpc.InternalGrpcActorInterface
import com.mint.db.raft.model.Command
import com.mint.db.raft.model.CommandResult
import java.util.function.BiConsumer

class InternalGrpcActorMock(private val actions: ActionSink, private val nProcess: Int) : InternalGrpcActorInterface {
    override fun sendVoteRequest(
        voteRequest: Raft.VoteRequest,
        onRequestVoteResult: BiConsumer<Int, Raft.VoteResponse>
    ) {
        for (destId in 1..nProcess) {
            actions += ProcessAction.Send(destId, voteRequest)
        }
    }

    override fun sendAppendEntriesRequest(
        appendEntriesRequest: Raft.AppendEntriesRequest,
        onAppendEntryResult: BiConsumer<Int, Raft.AppendEntriesResponse>
    ) {
        for (destId in 1..nProcess) {
            actions += ProcessAction.Send(destId, appendEntriesRequest)
        }
    }

    override fun sendAppendEntriesRequest(
        destId: Int,
        appendEntriesRequest: Raft.AppendEntriesRequest,
        onAppendEntryResult: BiConsumer<Int, Raft.AppendEntriesResponse>
    ) {
        actions += ProcessAction.Send(destId, appendEntriesRequest)
    }

    override fun sendClientCommand(
        destId: Int,
        command: Command,
        onCommandResult: BiConsumer<Command, CommandResult>?
    ) {
        val message = Raft.ClientCommandRequestRPC.newBuilder()
            .setOperation(Raft.Operation.PUT)
            .setKey(ByteString.copyFromUtf8(command.key()))
            .setValue(ByteString.copyFromUtf8(command.value()))
            .setUncommitted(false)
            .setProcessId(command.processId().toInt())
            .build()
        actions += ProcessAction.Send(destId, message)
    }
}
