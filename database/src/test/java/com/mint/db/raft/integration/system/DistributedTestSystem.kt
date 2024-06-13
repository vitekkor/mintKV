package com.mint.db.raft.integration.system

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.google.inject.Guice
import com.google.inject.util.Modules
import com.mint.db.config.InjectionModule
import com.mint.db.config.NodeConfig
import com.mint.db.grpc.server.Server
import com.mint.db.raft.StateMachine
import com.mint.db.raft.integration.configuration.Configuration
import com.mint.db.raft.integration.configuration.IntegrationTestInjectionModule
import com.mint.db.raft.integration.system.ActionTag.COMMIT
import com.mint.db.raft.integration.system.ActionTag.DUMP
import com.mint.db.raft.integration.system.ActionTag.ERROR
import com.mint.db.raft.integration.system.ActionTag.LISTENING
import com.mint.db.raft.integration.system.ActionTag.RESTART
import com.mint.db.raft.integration.system.ActionTag.RESULT
import com.mint.db.raft.model.Command
import com.mint.db.raft.model.CommandResult
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.foreign.MemorySegment
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock


data class SystemCommandResult(
    val processId: Int,
    val result: CommandResult
)

private const val AWAIT_TIMEOUT_MS = 5000L
private const val NODE_CONFIG_ENV_PROPERTY = "mint.config.location"

enum class ActionTag { LISTENING, DUMP, RESULT, ERROR, RESTART, COMMIT }

class DistributedTestSystem {
    private val log: Logger = LoggerFactory.getLogger("System")
    private val procs = ConcurrentHashMap<Int, NodeProcess>()

    @Volatile
    private var failed = false

    private val sysLock = ReentrantLock()
    private val sysCond = sysLock.newCondition()

    private val listening = BooleanArray(Configuration.nProcesses)
    private val dump = arrayOfNulls<StateMachine<MemorySegment>>(Configuration.nProcesses)
    private val commit = arrayOfNulls<Command?>(Configuration.nProcesses)
    private val restart = BooleanArray(Configuration.nProcesses)
    private val results = ArrayDeque<SystemCommandResult>()

    private val executor = Executors.newFixedThreadPool(
        Configuration.nProcesses,
        ThreadFactoryBuilder().setNameFormat("node-%d").build()
    )

    init {
        log.info("Starting ${Configuration.nProcesses} processes")
        for (node in 0 until Configuration.nProcesses) startProcess(node)
        Runtime.getRuntime().addShutdownHook(Thread {
            for (proc in procs.values) proc.stop()
        })
    }

    private fun startProcess(node: Int) {
        if (procs.containsKey(node)) return // already active
        executor.execute {
            val injector = Guice.createInjector(
                Modules.override(InjectionModule()).with(IntegrationTestInjectionModule(this, node))
            )
            val grpcServer: Server
            synchronized(this) {
                System.setProperty(NODE_CONFIG_ENV_PROPERTY, Configuration.nodes[node])
                val conf = injector.getInstance(NodeConfig::class.java)
                conf.setHeartbeatRandom(false)
                conf.heartbeatTimeoutMs = 1000

                grpcServer = injector.getInstance(Server::class.java)
                grpcServer.start()
            }
            procs[node] = NodeProcess(
                node,
                NodeGrpcClient("localhost:808$node"),
                grpcServer,
                injector,
                this
            )
            this.onAction(node, LISTENING)
        }
    }


    fun checkNotFailed() {
        check(!failed) { "The test had failed" }
    }

    fun request(id: Int, command: Command) {
        procById(id)?.request(command)
    }

    fun reqExit() {
        log.info("Requesting stop for all nodes")
        executor.shutdownNow()
        reqAll { it.stop() }
    }

    fun reqAll(action: (NodeProcess) -> Unit) {
        procs.values.forEach {
            action(it)
        }
    }

    private fun procById(id: Int) = procs[id]

    private fun <T> await(
        condition: () -> Boolean,
        action: () -> T,
        message: String,
        awaitTimeout: Long? = null
    ): T = sysLock.withLock {
        val deadline = System.currentTimeMillis() + (awaitTimeout ?: AWAIT_TIMEOUT_MS)
        while (!condition()) {
            checkNotFailed()
            val now = System.currentTimeMillis()
            if (now >= deadline)
                error("Test timed out waiting for $message")
            sysCond.await(deadline - now, TimeUnit.MILLISECONDS)
        }
        action()
    }

    fun awaitListening() = await(
        condition = { listening.drop(1).all { it } },
        action = { listening.fill(false) },
        message = "listening",
        awaitTimeout = 10_000
    )

    fun awaitDump(id: Int) = await(
        condition = { dump[id] != null },
        action = { dump[id]!!.also { dump[id] = null } },
        message = "dump $id"
    )

    fun awaitCommit(id: Int) = await(
        condition = { commit[id] != null },
        action = { commit[id]!!.also { commit[id] = null } },
        message = "commit $id"
    )

    fun awaitClientCommandResult(): SystemCommandResult = await(
        condition = { results.isNotEmpty() },
        action = { results.removeFirst() },
        message = "client command result",
        awaitTimeout = 100_000
    )

    fun awaitRestart(id: Int) = await(
        condition = { restart[id] },
        action = { restart[id] = false },
        message = "restart $id"
    )

    @Suppress("UNCHECKED_CAST")
    fun onAction(
        id: Int,
        actionTag: ActionTag,
        command: Command? = null,
        commandResult: CommandResult? = null,
        stateMachine: StateMachine<*>? = null
    ) = sysLock.withLock {
        when (actionTag) {
            LISTENING -> {
                listening[id] = true
                sysCond.signalAll()
            }

            DUMP -> {
                dump[id] = stateMachine as StateMachine<MemorySegment>
                sysCond.signalAll()
            }

            COMMIT -> {
                commit[id] = command
                sysCond.signalAll()
            }

            RESULT -> {
                results += SystemCommandResult(id, commandResult!!)
                sysCond.signalAll()
            }

            RESTART -> {
                restart[id] = true
                sysCond.signalAll()
            }

            ERROR -> error("Process $id reports error")
        }
    }

    fun restartNode(pid: Int) {
        procById(pid)?.restart()
    }
}
