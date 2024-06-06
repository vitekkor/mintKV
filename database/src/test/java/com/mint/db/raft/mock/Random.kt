package com.mint.db.raft.mock

import com.mint.db.dao.impl.BaseEntry
import com.mint.db.dao.impl.StringDaoWrapper
import com.mint.db.raft.Environment
import com.mint.db.raft.model.InsertCommand
import com.mint.db.raft.model.InsertCommandResult
import com.mint.db.raft.model.LogId
import com.mint.db.replication.model.impl.BaseLogEntry
import com.mint.db.replication.model.impl.OperationType
import java.lang.foreign.MemorySegment
import kotlin.random.Random
import kotlin.random.nextInt
import kotlin.random.nextLong

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

fun Random.nextMemorySegment() = StringDaoWrapper.toMemorySegment(randomStrings.random(this))

fun Random.nextCommand(processId: Long) =
    InsertCommand(processId, nextString(), nextString(), false)

fun Random.nextCommandResult() = InsertCommandResult(nextCommandId(), nextString())

fun Random.nextCommandId() = nextLong(10L..99L)

fun Random.nextLogEntry(index: Long, term: Long) =
    BaseLogEntry<MemorySegment>(
        OperationType.PUT,
        BaseEntry(
            nextMemorySegment(),
            nextMemorySegment(),
            null,
            false
        ),
        LogId(index, term)
    )
