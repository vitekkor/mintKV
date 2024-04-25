package com.mint.db.replication;

import com.mint.db.replication.model.LogEntry;

public interface ReplicatedLogManager<D> {
    void appendLogEntry(LogEntry<D> logEntry);
}
