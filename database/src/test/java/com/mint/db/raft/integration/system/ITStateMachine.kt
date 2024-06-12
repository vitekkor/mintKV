package com.mint.db.raft.integration.system

import com.mint.db.dao.Dao
import com.mint.db.dao.Entry
import com.mint.db.raft.DaoStateMachine
import com.mint.db.raft.model.Command
import com.mint.db.raft.model.CommandResult
import com.mint.db.replication.model.LogEntry
import java.lang.foreign.MemorySegment

class ITStateMachine(
    dao: Dao<MemorySegment, Entry<MemorySegment>>,
    private val system: DistributedTestSystem,
    private val nodeId: Int
) : DaoStateMachine(dao) {
    override fun apply(logEntry: LogEntry<MemorySegment>, committed: Boolean): CommandResult {
        system.onAction(nodeId, ActionTag.COMMIT, logEntry.command)
        return super.apply(logEntry, committed)
    }

    override fun apply(command: Command, currentTerm: Long): CommandResult {
        system.onAction(nodeId, ActionTag.COMMIT, command)
        return super.apply(command, currentTerm)
    }
}
