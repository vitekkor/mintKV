package com.mint.db.raft;

import com.mint.DatabaseServiceOuterClass.ReadMode;
import com.mint.db.dao.Dao;
import com.mint.db.dao.Entry;
import com.mint.db.dao.impl.BaseEntry;
import com.mint.db.dao.impl.StringDaoWrapper;
import com.mint.db.raft.model.Command;
import com.mint.db.raft.model.CommandResult;
import com.mint.db.raft.model.GetCommand;
import com.mint.db.raft.model.GetCommandResult;
import com.mint.db.raft.model.InsertCommand;
import com.mint.db.raft.model.InsertCommandResult;
import com.mint.db.replication.model.LogEntry;
import com.mint.db.replication.model.PersistentState;
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
                String entryValue = null;
                if (entry != null) {
                    entryValue = toString(committed ? entry.committedValue() : entry.readUncommittedValue());
                }

                commandResult = new GetCommandResult(
                        logEntry.logId().term(),
                        toString(key),
                        entryValue
                );
            }
            case OperationType.PUT, OperationType.DELETE -> {
                MemorySegment committedValue = operationType == OperationType.PUT ? value : null;

                if (committed) {
                    dao.upsert(new BaseEntry(key, committedValue, null, false));
                } else {
                    Entry<MemorySegment> oldEntry = dao.get(key);
                    MemorySegment oldValue = oldEntry != null ? oldEntry.committedValue() : null;
                    dao.upsert(new BaseEntry(key, oldValue, committedValue, true));
                }

                commandResult = new InsertCommandResult(logEntry.logId().term(), toString(key));

            }
            default -> {

            }
        }

        return commandResult;
    }

    @Override
    public CommandResult apply(Command command, long currentTerm) {
        switch (command) {
            case GetCommand getCommand -> {
                Entry<MemorySegment> entry = dao.get(StringDaoWrapper.toMemorySegment(getCommand.key()));
                String entryValue = null;
                if (entry != null) {
                    entryValue = toString(
                            getCommand.readMode() == ReadMode.READ_COMMITTED
                                    ? entry.committedValue()
                                    : entry.readUncommittedValue()
                    );
                }

                return new GetCommandResult(
                        currentTerm,
                        getCommand.key(),
                        entryValue
                );
            }
            case InsertCommand insertCommand -> {
                MemorySegment committedValue = StringDaoWrapper.toMemorySegment(insertCommand.value());
                MemorySegment key = StringDaoWrapper.toMemorySegment(insertCommand.key());

                if (insertCommand.uncommitted()) {
                    Entry<MemorySegment> oldEntry = dao.get(key);
                    MemorySegment oldValue = oldEntry != null ? oldEntry.committedValue() : null;
                    dao.upsert(new BaseEntry(key, oldValue, committedValue, true));
                } else {
                    dao.upsert(new BaseEntry(key, committedValue, null, false));
                }

                return new InsertCommandResult(currentTerm, toString(key));
            }
            case null -> throw new IllegalArgumentException("Command is null");
        }
    }

    public static String toString(MemorySegment memorySegment) {
        return memorySegment == null ? null :
                new String(memorySegment.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DaoStateMachine that)) return false;

        return dao.equals(that.dao);
    }

    @Override
    public int hashCode() {
        return dao.hashCode();
    }
}
