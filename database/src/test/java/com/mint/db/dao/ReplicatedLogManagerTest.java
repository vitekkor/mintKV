package com.mint.db.dao;

import com.mint.db.config.ConfigParser;
import com.mint.db.dao.impl.StringDaoWrapper;
import com.mint.db.replication.impl.ReplicatedLogManagerImpl;
import com.mint.db.replication.model.LogEntry;
import com.mint.db.replication.model.PersistentState;
import com.mint.db.replication.model.impl.OperationType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.List;

import static com.mint.db.replication.impl.ReplicatedLogManagerImpl.createLogEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ReplicatedLogManagerTest {
    private static ReplicatedLogManagerImpl createReplicatedLogManager() throws IOException {
        return new ReplicatedLogManagerImpl(ConfigParser.parseConfig(), new PersistentState());
    }

    @Test
    @DisplayName("Test append log entry")
    public void testAppendLogEntry() throws IOException {
        ReplicatedLogManagerImpl logManager = createReplicatedLogManager();
        LogEntry<MemorySegment> logEntry = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key"),
                StringDaoWrapper.toMemorySegment("value"),
                System.currentTimeMillis(),
                0
        );
        logManager.appendLogEntry(logEntry);

        LogEntry<MemorySegment> deserializedLogEntry = deserializeLogEntry(logManager);
        checkLogEntry(logEntry, deserializedLogEntry);

    }

    @Test
    @DisplayName("Test with null value")
    public void testAppendLogEntryWithNullValue() throws IOException {
        ReplicatedLogManagerImpl logManager = createReplicatedLogManager();
        LogEntry<MemorySegment> logEntry = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key"),
                null,
                System.currentTimeMillis(),
                0
        );
        logManager.appendLogEntry(logEntry);

        LogEntry<MemorySegment> deserializedLogEntry = deserializeLogEntry(logManager);
        assertEquals(logEntry.operationType(), deserializedLogEntry.operationType());

    }

    @Test
    @DisplayName("Test with 2 values")
    public void testAppendLogEntryWithTwoValues() throws IOException {
        ReplicatedLogManagerImpl logManager = createReplicatedLogManager();
        LogEntry<MemorySegment> logEntry = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key"),
                StringDaoWrapper.toMemorySegment("value"),
                System.currentTimeMillis(),
                0
        );
        logManager.appendLogEntry(logEntry);
        LogEntry<MemorySegment> logEntry2 = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key2"),
                StringDaoWrapper.toMemorySegment("value2"),
                System.currentTimeMillis(),
                1
        );
        logManager.appendLogEntry(logEntry2);

        List<LogEntry<MemorySegment>> deserializedLogEntries = logManager.readLog(0, 2);
        checkLogEntry(logEntry, deserializedLogEntries.get(0));
        checkLogEntry(logEntry2, deserializedLogEntries.get(1));

    }

    @Test
    @DisplayName("Test write logs and read first log entry")
    public void testWriteLogsAndReadFirstLogEntry() throws IOException {
        ReplicatedLogManagerImpl logManager = createReplicatedLogManager();
        LogEntry<MemorySegment> logEntry = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key"),
                StringDaoWrapper.toMemorySegment("value"),
                System.currentTimeMillis(),
                0
        );
        logManager.appendLogEntry(logEntry);
        LogEntry<MemorySegment> logEntry2 = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key2"),
                StringDaoWrapper.toMemorySegment("value2"),
                System.currentTimeMillis(),
                1
        );
        logManager.appendLogEntry(logEntry2);


        LogEntry<MemorySegment> deserializedLogEntry = deserializeLogEntry(logManager);
        checkLogEntry(logEntry, deserializedLogEntry);

        LogEntry<MemorySegment> logEntry3 = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key3"),
                StringDaoWrapper.toMemorySegment("value3"),
                System.currentTimeMillis(),
                2
        );
        logManager.appendLogEntry(logEntry3);

        deserializedLogEntry = logManager.readLog(2, 3).getFirst();
        checkLogEntry(logEntry3, deserializedLogEntry);
    }

    @Test
    @DisplayName("Test rollback")
    public void testRollback() throws IOException, InterruptedException {
        ReplicatedLogManagerImpl logManager = createReplicatedLogManager();
        LogEntry<MemorySegment> logEntry = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key"),
                StringDaoWrapper.toMemorySegment("value"),
                0,
                0
        );
        logManager.appendLogEntry(logEntry);
        LogEntry<MemorySegment> logEntry2 = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key2"),
                StringDaoWrapper.toMemorySegment("value2"),
                1,
                1
        );
        logManager.appendLogEntry(logEntry2);
        LogEntry<MemorySegment> logEntry3 = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key3"),
                StringDaoWrapper.toMemorySegment("value3"),
                2,
                2
        );
        logManager.appendLogEntry(logEntry3);
        LogEntry<MemorySegment> logEntry4 = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key4"),
                StringDaoWrapper.toMemorySegment("value4"),
                1,
                3
        );
        logManager.appendLogEntry(logEntry4);

        var entries = logManager.deserializeLogEntries(0, 10);
        assertEquals(2, entries.size());

        LogEntry<MemorySegment> logEntry5 = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key5"),
                StringDaoWrapper.toMemorySegment("value5"),
                2,
                4
        );
        logManager.appendLogEntry(logEntry5);

        entries = logManager.deserializeLogEntries(0, 10);
        assertEquals(3, entries.size());

        entries = logManager.readLog(1, 2);
        assertEquals(1, entries.size());
    }

    private LogEntry<MemorySegment> deserializeLogEntry(ReplicatedLogManagerImpl logManager) throws IOException {
        return logManager.readLog(0, 1).getFirst();
    }

    private void checkLogEntry(LogEntry<MemorySegment> expected, LogEntry<MemorySegment> actual) {
        assertEquals(expected.operationType(), actual.operationType());
        assertEquals(expected.entry().key().byteSize(), actual.entry().key().byteSize());
        assertEquals(expected.entry().key().asByteBuffer().get(), actual.entry().key().asByteBuffer().get());
        if (expected.entry().value() == null) {
            assertNull(actual.entry().value());
        } else {
            assertEquals(expected.entry().value().byteSize(), actual.entry().value().byteSize());
            assertEquals(expected.entry().value().asByteBuffer().get(), actual.entry().value().asByteBuffer().get());
        }
        assertEquals(expected.logId(), actual.logId());
    }
}
