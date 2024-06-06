package com.mint.db.raft.mock

import com.mint.db.dao.Dao
import com.mint.db.dao.Entry
import com.mint.db.raft.DaoStateMachine
import com.mint.db.raft.model.Command
import com.mint.db.raft.model.CommandResult
import com.mint.db.replication.model.LogEntry
import java.lang.foreign.MemorySegment

class MockDaoStateMachine(
    private val actions: ActionSink,
    dao: Dao<MemorySegment, Entry<MemorySegment>>
) : DaoStateMachine(dao) {
    override fun apply(logEntry: LogEntry<MemorySegment>, committed: Boolean): CommandResult {
        actions += ProcessAction.ApplyCommandLogEntry(logEntry, committed)
        return super.apply(logEntry, committed)
    }

    override fun apply(command: Command, currentTerm: Long): CommandResult {
        actions += ProcessAction.ApplyCommand(command, currentTerm)
        return super.apply(command, currentTerm)
    }
}
