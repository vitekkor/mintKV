package com.mint.db.raft.mock

import com.mint.db.dao.impl.BaseEntry
import com.mint.db.dao.impl.StringDaoWrapper
import com.mint.db.raft.Environment
import com.mint.db.raft.model.Command
import com.mint.db.raft.model.GetCommand
import com.mint.db.raft.model.InsertCommand
import com.mint.db.raft.model.InsertCommandResult
import com.mint.db.raft.model.LogId
import com.mint.db.replication.model.LogEntry
import com.mint.db.replication.model.impl.BaseLogEntry
import com.mint.db.replication.model.impl.OperationType
import java.lang.foreign.MemorySegment
import kotlin.random.Random
import kotlin.random.nextInt

private val randomStrings = run {
    val rnd = Random(1)
    List(10) {
        buildString {
            repeat(rnd.nextInt(1..3)) {
                append('A' + rnd.nextInt(26))
            }
        }
    }
}

fun Random.nextString() = randomStrings.random(this)

fun Random.nextMemorySegment(): MemorySegment = StringDaoWrapper.toMemorySegment(randomStrings.random(this))

fun Random.nextCommand(processId: Int, uncommitted: Boolean = false) =
    InsertCommand(processId, nextString(), nextString(), uncommitted)

fun Random.nextCommand(processId: Int, id: Int, uncommitted: Boolean = false) =
    InsertCommand(processId, "${id}_${nextString()}", "${id}_${nextString()}", uncommitted)

fun Random.nextCommandResult(term: Long) = InsertCommandResult(term, nextString())

fun InsertCommandResult.toDummyCommand(pid: Int) = InsertCommand(pid, key, "value_$key", false)

fun Random.nextLogEntry(index: Long, term: Long, env: Environment<*>) =
    BaseLogEntry(
        OperationType.PUT,
        BaseEntry(
            nextMemorySegment(),
            nextMemorySegment(),
            null,
            false
        ),
        LogId(index, term),
        nextInt(0 until env.numberOfProcesses())
    )

fun Command.toLogEntry(logId: LogId): LogEntry<MemorySegment> {
    val operationType = when (this) {
        is InsertCommand -> if (value != null ) OperationType.PUT else OperationType.DELETE
        is GetCommand -> OperationType.GET
    }
    return BaseLogEntry(
        operationType,
        BaseEntry(
            StringDaoWrapper.toMemorySegment(key()),
            StringDaoWrapper.toMemorySegment(value()),
            null,
            false
        ),
        logId,
        processId()
    )
}
