package com.mint.db.replication.model.impl;

import com.mint.db.dao.Entry;
import com.mint.db.dao.impl.BaseEntry;
import com.mint.db.raft.model.Command;
import com.mint.db.raft.model.LogId;
import com.mint.db.replication.model.LogEntry;

import java.lang.foreign.MemorySegment;

public record BaseLogEntry<D>(
        OperationType operationType,
        Entry<D> entry,
        LogId logId
) implements LogEntry<D> {
    public static BaseLogEntry<MemorySegment> valueOf(Raft.LogEntry entry) {
        return new BaseLogEntry<>(
                OperationType.valueOf(entry.getOperation().name()),
                BaseEntry.valueOf(entry),
                new LogId(entry.getIndex(), entry.getTerm())
        );
    }

    @Override
    public String toString() {
        return STR."{ operationType=\{operationType}, entry=\{entry}, logId=\{logId} }";
    }

    @Override
    public Command getCommand() {
        // TODO implement
        return null;
    }
}
