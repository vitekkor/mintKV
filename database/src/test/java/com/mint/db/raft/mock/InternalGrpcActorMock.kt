package com.mint.db.raft.mock

import com.google.protobuf.ByteString
import com.mint.db.Raft
import com.mint.db.Raft.ClientCommandResponseRPC
import com.mint.db.grpc.InternalGrpcActorInterface
import com.mint.db.raft.model.Command
import com.mint.db.raft.model.CommandResult
import com.mint.db.util.ClientCommandResultConsumer
import io.grpc.stub.StreamObserver
import java.util.function.BiConsumer

class InternalGrpcActorMock(
    private val actions: ActionSink,
    nProcess: Int,
    raftActorId: Int
) : InternalGrpcActorInterface {
    private val commandStreamObserverMap = HashMap<Command, StreamObserver<*>>()

    private val clusterNodes = (0 until nProcess).filter { it != raftActorId }
    override fun sendVoteRequest(
        voteRequest: Raft.VoteRequest,
        onRequestVoteResult: BiConsumer<Int, Raft.VoteResponse>
    ) {
        for (destId in clusterNodes) {
            actions += ProcessAction.Send(destId, voteRequest)
        }
    }

    override fun sendAppendEntriesRequest(
        appendEntriesRequest: Raft.AppendEntriesRequest,
        onAppendEntryResult: BiConsumer<Int, Raft.AppendEntriesResponse>
    ) {
        for (destId in clusterNodes) {
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
        onCommandResult: ClientCommandResultConsumer
    ) {
        val message = Raft.ClientCommandRequestRPC.newBuilder()
            .setOperation(Raft.Operation.PUT)
            .setKey(ByteString.copyFromUtf8(command.key()))
            .setValue(ByteString.copyFromUtf8(command.value()))
            .setUncommitted(false)
            .setProcessId(command.processId())
            .build()
        actions += ProcessAction.Send(destId, message)
    }

    override fun addClientCommandCallback(command: Command, responseObserver: StreamObserver<*>) {
        // empty
    }

    override fun onClientCommandResult(command: Command, commandResult: CommandResult) {
        val response = ClientCommandResponseRPC.newBuilder()
            .setTerm(commandResult.term())
            .setKey(ByteString.copyFromUtf8(commandResult.key()))
            .setValue(if (commandResult.value() != null) ByteString.copyFromUtf8(commandResult.value()) else ByteString.EMPTY)
            .build()
        actions += ProcessAction.Send(command.processId(), response)
    }
}
