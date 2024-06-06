package com.mint.db.raft.mock

import com.mint.db.config.NodeConfig
import com.mint.db.dao.Dao
import com.mint.db.dao.Entry
import com.mint.db.replication.impl.ReplicatedLogManagerImpl
import com.mint.db.replication.model.LogEntry
import com.mint.db.replication.model.PersistentState
import java.lang.foreign.MemorySegment
import kotlin.random.Random
import kotlin.random.nextLong

class MockReplicatedLogManager(
    private val actions: ActionSink,
    nodeConfig: NodeConfig,
    state: PersistentState,
    dao: Dao<MemorySegment, Entry<MemorySegment>>,
    lastLogIndex: Long,
    rnd: Random,
) : ReplicatedLogManagerImpl(nodeConfig, state, dao) {
    init {
        var term = 1L
        for (index in 1L..lastLogIndex) {
            term = rnd.nextLong(term..readPersistentState().currentTerm)
            super.appendLogEntry(rnd.nextLogEntry(index, term))
        }
    }

    override fun writePersistentState(state: PersistentState) {
        if (readPersistentState() == state) return
        super.writePersistentState(state)
        actions.removeActionIf { it is ProcessAction.WritePersistentState }
        actions += ProcessAction.WritePersistentState(state)
    }

    override fun appendLogEntry(logEntry: LogEntry<MemorySegment>) {
        super.appendLogEntry(logEntry)
        actions += ProcessAction.AppendLogEntry(logEntry)
    }

    override fun toString(): String = buildString {
        for (index in 1..readLastLogId().index) {
            if (index > 1) append("\n")
            append(readLog(index))
        }
    }
}
