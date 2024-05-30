package com.mint.db.replication;

import com.mint.db.raft.model.LogId;
import com.mint.db.replication.model.LogEntry;
import com.mint.db.replication.model.PersistentState;

import java.util.List;

public interface ReplicatedLogManager<D> {
    /**
     * Reads {@link PersistentState} of the Raft algorithm.
     */
    PersistentState readPersistentState();

    /**
     * Saves {@link PersistentState}of the Raft algorithm.
     */
    void writePersistentState(PersistentState state);

    void appendLogEntry(LogEntry<D> logEntry);

    /**
     * Reads identifier of the last log entry.
     */
    LogId readLastLogId();

    /**
     * Reads log entry at the specified index, return `null` if the entry is not present.
     */
    List<LogEntry<D>> readLog(long fromIndex, long toIndex);

    long commitIndex();
}
