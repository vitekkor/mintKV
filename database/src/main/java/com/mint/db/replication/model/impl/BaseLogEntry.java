package com.mint.db.replication.model.impl;

import com.mint.DatabaseServiceOuterClass;
import com.mint.db.Raft;
import com.mint.db.dao.Entry;
import com.mint.db.dao.impl.BaseEntry;
import com.mint.db.dao.impl.StringDaoWrapper;
import com.mint.db.raft.model.Command;
import com.mint.db.raft.model.GetCommand;
import com.mint.db.raft.model.InsertCommand;
import com.mint.db.raft.model.LogId;
import com.mint.db.replication.model.LogEntry;

import java.lang.foreign.MemorySegment;

public record BaseLogEntry<D>(
        OperationType operationType,
        Entry<D> entry,
        LogId logId,
        int processId
) implements LogEntry<D> {
    public static BaseLogEntry<MemorySegment> valueOf(Raft.LogEntry entry) {
        return new BaseLogEntry<>(
                OperationType.valueOf(entry.getOperation().name()),
                BaseEntry.valueOf(entry),
                new LogId(entry.getIndex(), entry.getTerm()),
                entry.getProcessId()
        );
    }

    private static String entryToString(Object entryField) {
        if (entryField instanceof MemorySegment memorySegment) {
            return StringDaoWrapper.toString(memorySegment);
        }
        return entryField.toString();
    }

    @Override
    public Command getCommand() {
        return switch (operationType) {
            case PUT, DELETE -> new InsertCommand(
                    processId,
                    entryToString(entry.key()),
                    entryToString(entry.readUncommittedValue()),
                    entry.uncommittedValueIsNotNull()
            );
            case GET -> new GetCommand(
                    processId,
                    entryToString(entry.key()),
                    DatabaseServiceOuterClass.ReadMode.READ_CONSENSUS
            );
        };
    }
}
