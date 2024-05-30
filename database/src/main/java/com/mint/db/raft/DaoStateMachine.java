package com.mint.db.raft;

import com.mint.db.dao.Dao;
import com.mint.db.dao.Entry;
import com.mint.db.dao.impl.BaseEntry;
import com.mint.db.raft.model.CommandResult;
import com.mint.db.raft.model.GetCommandResult;
import com.mint.db.raft.model.InsertCommandResult;
import com.mint.db.replication.model.LogEntry;
import com.mint.db.replication.model.impl.OperationType;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

public class DaoStateMachine implements StateMachine<MemorySegment> {
    private final Dao<MemorySegment, Entry<MemorySegment>> dao;

    public DaoStateMachine(Dao<MemorySegment, Entry<MemorySegment>> dao) {
        this.dao = dao;
    }

    @Override
    public CommandResult apply(LogEntry<MemorySegment> logEntry, boolean committed) {
        OperationType operationType = logEntry.operationType();

        MemorySegment key = logEntry.entry().key();
        MemorySegment value = logEntry.entry().committedValue();

        CommandResult commandResult = null;
        switch (operationType) {
            case OperationType.GET -> {
                Entry<MemorySegment> entry = dao.get(logEntry.entry().key());

                commandResult = new GetCommandResult(
                        logEntry.logId().term(),
                        toString(entry.key()),
                        toString(entry.committedValue())
                );
            }
            case OperationType.PUT -> {
                if (committed) {
                    dao.upsert(new BaseEntry<>(key, value, null, false));
                } else {
                    Entry<MemorySegment> oldEntry = dao.get(key);
                    MemorySegment oldValue = oldEntry != null ? oldEntry.committedValue() : null;
                    dao.upsert(new BaseEntry<>(key, oldValue, value, true));
                }

                commandResult = new InsertCommandResult(logEntry.logId().term(), toString(key));
            }
            case OperationType.DELETE -> {
                if (committed) {
                    dao.upsert(new BaseEntry<>(key, null, null, false));
                } else {
                    Entry<MemorySegment> oldEntry = dao.get(key);
                    MemorySegment oldValue = oldEntry != null ? oldEntry.committedValue() : null;
                    dao.upsert(new BaseEntry<>(key, oldValue, null, true));
                }

                commandResult = new InsertCommandResult(logEntry.logId().term(), toString(key));
            }
            default -> {

            }
        }

        return commandResult;
    }

    public static String toString(MemorySegment memorySegment) {
        return memorySegment == null ? null :
                new String(memorySegment.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
    }
}
