package com.mint.db.replication.model.impl;

import com.mint.db.Entry;
import com.mint.db.Raft;
import com.mint.db.impl.BaseEntry;
import com.mint.db.replication.model.LogEntry;

import java.lang.foreign.MemorySegment;

public record BaseLogEntry<D>(OperationType operationType, Entry<D> entry, long timestamp) implements LogEntry<D> {
    public static BaseLogEntry<MemorySegment> valueOf(Raft.LogEntry entry) {
        return new BaseLogEntry<>(
                OperationType.valueOf(entry.getOperation().name()),
                BaseEntry.valueOf(entry),
                System.currentTimeMillis()
        );
    }

    @Override
    public String toString() {
        return STR."{ operationType=\{operationType}, entry=\{entry}, timestamp=\{timestamp} }";
    }

    @Override
    public long term() {
        return 0;
    }
}
