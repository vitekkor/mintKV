package com.mint.db.raft.integration.system

import com.google.inject.Injector
import com.google.inject.Key
import com.google.protobuf.ByteString
import com.mint.DatabaseServiceOuterClass
import com.mint.db.grpc.InternalGrpcActor
import com.mint.db.grpc.server.Server
import com.mint.db.raft.DaoStateMachine
import com.mint.db.raft.RaftActor
import com.mint.db.raft.StateMachine
import com.mint.db.raft.integration.configuration.annotations.TestStateMachineBean
import com.mint.db.raft.model.Command
import com.mint.db.raft.model.GetCommand
import com.mint.db.raft.model.GetCommandResult
import com.mint.db.raft.model.InsertCommand
import com.mint.db.raft.model.InsertCommandResult
import com.mint.db.util.LogUtil
import org.slf4j.LoggerFactory
import java.lang.foreign.MemorySegment

class NodeProcess(
    private val id: Int,
    private val nodeGrpcClient: NodeGrpcClient,
    private val grpcServer: Server,
    private val injector: Injector,
    private val distributedTestSystem: DistributedTestSystem,
) {
    private val log = LoggerFactory.getLogger("""${NodeProcess::class.java.name}_$id""")

    fun request(command: Command) {
        log.info("out.${id} >> $command")
        when (command) {
            is InsertCommand -> {
                if (command.value != null) {
                    val request = DatabaseServiceOuterClass.InsertRequest.newBuilder()
                        .setKey(command.key)
                        .setValue(command.value)
                        .setUncommitted(command.uncommitted)
                        .build()
                    nodeGrpcClient.insert(request) {
                        log.info("Node insert response: ${LogUtil.protobufMessageToString(it)}")
                        distributedTestSystem.onAction(
                            id,
                            ActionTag.RESULT,
                            commandResult = InsertCommandResult(0, command.key)
                        )
                    }
                } else {
                    val request = DatabaseServiceOuterClass.DeleteRequest.newBuilder()
                        .setKey(command.key)
                        .setUncommitted(command.uncommitted)
                        .build()
                    nodeGrpcClient.delete(request) {
                        log.info("Node delete response: ${LogUtil.protobufMessageToString(it)}")
                        distributedTestSystem.onAction(
                            id,
                            ActionTag.RESULT,
                            commandResult = InsertCommandResult(0, command.key) // FIXME
                        )
                    }
                }
            }

            is GetCommand -> {
                val request = DatabaseServiceOuterClass.GetRequest.newBuilder()
                    .setKey(ByteString.copyFromUtf8(command.key))
                    .setMode(command.readMode)
                    .build()
                nodeGrpcClient.get(request) {
                    log.info("Node get response: ${LogUtil.protobufMessageToString(it)}")
                    distributedTestSystem.onAction(
                        id,
                        ActionTag.RESULT,
                        commandResult = GetCommandResult(
                            0, // FIXME
                            command.key,
                            if (it.found) it.value.toStringUtf8() else null
                        )
                    )
                }
            }
        }
    }

    fun restart() {
        grpcServer.forceStop()
        grpcServer.start()
        distributedTestSystem.onAction(id, ActionTag.RESTART)
    }

    fun stop() {
        grpcServer.forceStop()
        injector.getInstance(RaftActor::class.java).close()
        injector.getInstance(InternalGrpcActor::class.java).close()
        nodeGrpcClient.close()
    }

    @Suppress("UNCHECKED_CAST")
    fun dump() {
        val stateMachine: StateMachine<MemorySegment> =
            injector.getInstance(
                Key.get(
                    DaoStateMachine::class.java.genericInterfaces[0],
                    TestStateMachineBean::class.java
                )
            ) as StateMachine<MemorySegment>
        distributedTestSystem.onAction(id, ActionTag.DUMP, stateMachine = stateMachine)
    }
}
