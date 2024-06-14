package com.mint.db.raft.integration

import com.mint.db.dao.impl.BaseDao
import com.mint.db.raft.DaoStateMachine
import com.mint.db.raft.integration.configuration.Configuration
import com.mint.db.raft.integration.system.DistributedTestSystem
import com.mint.db.raft.mock.nextCommand
import com.mint.db.raft.model.CommandResult
import com.mint.db.raft.model.InsertCommand
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.File
import kotlin.random.Random
import kotlin.random.nextInt
import kotlin.test.assertEquals


class IntegrationTest {
    private val sys = DistributedTestSystem()
    private val nProcesses = Configuration.nodes.size
    private val rnd = Random(1)
    private val expectedMachine = DaoStateMachine(BaseDao())
    private var lastCommandId = 100

    @BeforeEach
    fun setup() {
        sys.awaitListening()
    }

    @AfterEach
    fun tearDown() {
        sys.reqExit()
        sys.checkNotFailed()
        for (i in 0..Configuration.nProcesses) {
            File("db_$i").deleteRecursively()
        }
    }

    private fun checkDumpsAtTheEnd() {
        sys.checkNotFailed()
        // execute a command to commit all state machines
        val command = InsertCommand(0, "0", "0", false)
        val expectedResult = expectedMachine.apply(command, 0)
        sys.request(0, command)
        val result = sys.awaitClientCommandResult(command)
        assertEquals(expectedResult, result.result)
        // wait until all processes have committed this command
        for (id in 0 until nProcesses) {
            do {
                val lastCommitted = sys.awaitCommit(id)
            } while (lastCommitted != command)
        }
        // now check dumps
        sys.reqAll { it.dump() }
        for (id in 0 until nProcesses) {
            val machine = sys.awaitDump(id)
            assertEquals(expectedMachine, machine, "State machine of process $id")
        }
    }

    /**
     * A basic test, no restarts.
     */
    @Test
    fun testCommandsOneByOne() {
        repeat(100) {
            performRandomCommandsAndAwait(1)
        }
        checkDumpsAtTheEnd()
    }

    /**
     * A complicated test with restarts and command batches.
     */
    @Test
    fun testCommandsRestartsInBetween() {
        repeat(50) {
            // usually restart one process, but sometimes a random number
            val nRestarts = if (rnd.nextInt(4) == 0) rnd.nextInt(0..nProcesses) else 1
            val pids = (0 until nProcesses).shuffled(rnd).take(nRestarts)
            for (pid in pids) sys.restartNode(pid)
            for (pid in pids) sys.awaitRestart(pid)
            performRandomCommandsAndAwait(rnd.nextInt(1..3))
        }
        checkDumpsAtTheEnd()
    }

    private fun performRandomCommandsAndAwait(nCommands: Int) {
        val expectedProcessId = rnd.nextInt(0 until nProcesses)
        val commands = List(nCommands) {
            rnd.nextCommand(expectedProcessId, ++lastCommandId)
        }
        val expectedResults: MutableList<CommandResult> = commands.map { expectedMachine.apply(it, 0) }.toMutableList()
        for (command in commands) {
            sys.request(command.processId, command)
        }
        while (expectedResults.isNotEmpty()) {
            val (processId, result) = sys.awaitClientCommandResult(commands.first())
            assertEquals(expectedProcessId, processId)
            val expectedResult = expectedResults.find { it == result }
            assertEquals(expectedResult, result)
            expectedResults.remove(expectedResult)
        }
    }
}
