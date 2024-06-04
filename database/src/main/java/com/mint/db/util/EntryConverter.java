package com.mint.db.util;

import com.google.protobuf.ByteString;
import com.mint.db.Raft;
import com.mint.db.replication.model.LogEntry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public final class EntryConverter {
    private EntryConverter() {
    }

    public static Raft.LogEntry logEntryToRaftLogEntry(LogEntry<MemorySegment> logEntry) {
        ByteString key = ByteString.copyFrom(logEntry.entry().key().toArray(ValueLayout.JAVA_BYTE));
        ByteString value = logEntry.entry().committedValue() != null
                ? ByteString.copyFrom(logEntry.entry().committedValue().toArray(ValueLayout.JAVA_BYTE))
                : ByteString.EMPTY;
        Raft.Operation operation = Raft.Operation.valueOf(logEntry.operationType().name());
        return Raft.LogEntry.newBuilder()
                .setTerm(logEntry.logId().term())
                .setIndex(logEntry.logId().index())
                .setKey(key)
                .setValue(value)
                .setOperation(operation)
                .build();
    }
}
