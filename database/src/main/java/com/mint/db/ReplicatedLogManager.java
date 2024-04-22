package com.mint.db;

public interface ReplicatedLogManager<D> {
    void appendLogEntry(LogEntry<D> logEntry);
}
