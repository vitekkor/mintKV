package com.mint.db.dao;

import com.mint.db.config.ConfigParser;
import com.mint.db.dao.impl.StringDaoWrapper;
import com.mint.db.replication.impl.ReplicatedLogManagerImpl;
import com.mint.db.replication.model.LogEntry;
import com.mint.db.replication.model.PersistentState;
import com.mint.db.replication.model.impl.OperationType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;

import static com.mint.db.replication.impl.ReplicatedLogManagerImpl.createLogEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ReplicatedLogManagerTest {
    private static ReplicatedLogManagerImpl createReplicatedLogManager() throws IOException {
        return new ReplicatedLogManagerImpl(ConfigParser.parseConfig(), new PersistentState(), new StringDaoWrapper());
    }

    @AfterEach
    public void clearData() {
        try {
            Path logDir = Path.of(ConfigParser.parseConfig().getLogDir());
            if (Files.exists(logDir)) {
                Files.walkFileTree(logDir, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    @DisplayName("Test append log entry")
    public void testAppendLogEntry() throws IOException {
        ReplicatedLogManagerImpl logManager = createReplicatedLogManager();
        LogEntry<MemorySegment> logEntry = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key"),
                StringDaoWrapper.toMemorySegment("value"),
                null,
                0,
                0
        );
        logManager.appendLogEntry(logEntry);

        LogEntry<MemorySegment> deserializedLogEntry = deserializeLogEntry(logManager);
        checkLogEntry(logEntry, deserializedLogEntry);
        logManager.close();
    }

    @Test
    @DisplayName("Test with null value")
    public void testAppendLogEntryWithNullValue() throws IOException {
        ReplicatedLogManagerImpl logManager = createReplicatedLogManager();
        LogEntry<MemorySegment> logEntry = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key"),
                null,
                null,
                0,
                0
        );
        logManager.appendLogEntry(logEntry);

        LogEntry<MemorySegment> deserializedLogEntry = deserializeLogEntry(logManager);
        assertEquals(logEntry.operationType(), deserializedLogEntry.operationType());
        logManager.close();
    }

    @Test
    @DisplayName("Test with 2 values")
    public void testAppendLogEntryWithTwoValues() throws IOException {
        ReplicatedLogManagerImpl logManager = createReplicatedLogManager();
        LogEntry<MemorySegment> logEntry = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key"),
                StringDaoWrapper.toMemorySegment("value"),
                null,
                0,
                0
        );
        logManager.appendLogEntry(logEntry);
        LogEntry<MemorySegment> logEntry2 = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key2"),
                StringDaoWrapper.toMemorySegment("value2"),
                null,
                1,
                1
        );
        logManager.appendLogEntry(logEntry2);

        List<LogEntry<MemorySegment>> deserializedLogEntries = logManager.readLog(0, 2);
        checkLogEntry(logEntry, deserializedLogEntries.get(0));
        checkLogEntry(logEntry2, deserializedLogEntries.get(1));
        logManager.close();
    }

    @Test
    @DisplayName("Test write logs and read first log entry")
    public void testWriteLogsAndReadFirstLogEntry() throws IOException {
        ReplicatedLogManagerImpl logManager = createReplicatedLogManager();
        LogEntry<MemorySegment> logEntry = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key"),
                StringDaoWrapper.toMemorySegment("value"),
                null,
                0,
                0
        );
        logManager.appendLogEntry(logEntry);
        LogEntry<MemorySegment> logEntry2 = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key2"),
                StringDaoWrapper.toMemorySegment("value2"),
                null,
                1,
                1
        );
        logManager.appendLogEntry(logEntry2);


        LogEntry<MemorySegment> deserializedLogEntry = deserializeLogEntry(logManager);
        checkLogEntry(logEntry, deserializedLogEntry);

        LogEntry<MemorySegment> logEntry3 = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key3"),
                StringDaoWrapper.toMemorySegment("value3"),
                null,
                2,
                2
        );
        logManager.appendLogEntry(logEntry3);

        deserializedLogEntry = logManager.readLog(2, 3).getFirst();
        checkLogEntry(logEntry3, deserializedLogEntry);
        logManager.close();
    }

    @Test
    @DisplayName("Test rollback")
    public void testRollback() throws IOException, InterruptedException {
        ReplicatedLogManagerImpl logManager = createReplicatedLogManager();
        LogEntry<MemorySegment> logEntry = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key"),
                StringDaoWrapper.toMemorySegment("value"),
                null,
                0,
                0
        );
        logManager.appendLogEntry(logEntry);
        LogEntry<MemorySegment> logEntry2 = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key2"),
                StringDaoWrapper.toMemorySegment("value2"),
                null,
                1,
                1
        );
        logManager.appendLogEntry(logEntry2);
        LogEntry<MemorySegment> logEntry3 = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key3"),
                StringDaoWrapper.toMemorySegment("value3"),
                null,
                2,
                2
        );
        logManager.appendLogEntry(logEntry3);
        LogEntry<MemorySegment> logEntry4 = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key4"),
                StringDaoWrapper.toMemorySegment("value4"),
                null,
                1,
                3
        );
        logManager.appendLogEntry(logEntry4);

        var entries = logManager.deserializeLogEntries(0);
        assertEquals(2, entries.size());

        LogEntry<MemorySegment> logEntry5 = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key5"),
                StringDaoWrapper.toMemorySegment("value5"),
                null,
                2,
                4
        );
        logManager.appendLogEntry(logEntry5);

        entries = logManager.deserializeLogEntries(0);
        assertEquals(3, entries.size());

        entries = logManager.readLog(1, 2);
        assertEquals(1, entries.size());
        logManager.close();
    }

    @Test
    @DisplayName("Reopen log manager")
    public void testReopenLogManager() throws IOException {
        ReplicatedLogManagerImpl logManager = createReplicatedLogManager();
        LogEntry<MemorySegment> logEntry = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key"),
                StringDaoWrapper.toMemorySegment("value"),
                null,
                0,
                0
        );
        logManager.appendLogEntry(logEntry);
        logManager.close();

        ReplicatedLogManagerImpl logManager2 = createReplicatedLogManager();
        LogEntry<MemorySegment> deserializedLogEntry = deserializeLogEntry(logManager2);
        checkLogEntry(logEntry, deserializedLogEntry);
        logManager2.close();
    }

    @Test
    @DisplayName("Reopen log manager with rollback")
    public void testReopenLogManagerWithRollback() throws IOException {
        ReplicatedLogManagerImpl logManager = createReplicatedLogManager();
        LogEntry<MemorySegment> logEntry = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key"),
                StringDaoWrapper.toMemorySegment("value"),
                null,
                0,
                0
        );
        logManager.appendLogEntry(logEntry);
        logManager.close();

        ReplicatedLogManagerImpl logManager2 = createReplicatedLogManager();
        LogEntry<MemorySegment> logEntry2 = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key2"),
                StringDaoWrapper.toMemorySegment("value2"),
                null,
                1,
                1
        );
        logManager2.appendLogEntry(logEntry2);

        LogEntry<MemorySegment> logEntry3 = createLogEntry(
                OperationType.PUT,
                StringDaoWrapper.toMemorySegment("key3"),
                StringDaoWrapper.toMemorySegment("value3"),
                null,
                0,
                2
        );
        logManager2.appendLogEntry(logEntry3);
        logManager2.close();

        ReplicatedLogManagerImpl logManager3 = createReplicatedLogManager();
        var deserializedLogEntry = logManager3.readLog(0, 10);
        assertEquals(1, deserializedLogEntry.size());
        logManager3.close();
    }

    private LogEntry<MemorySegment> deserializeLogEntry(ReplicatedLogManagerImpl logManager) {
        return logManager.readLog(0, 1).getFirst();
    }

    private void checkLogEntry(LogEntry<MemorySegment> expected, LogEntry<MemorySegment> actual) {
        assertEquals(expected.operationType(), actual.operationType());
        assertEquals(expected.entry().key().byteSize(), actual.entry().key().byteSize());
        assertEquals(expected.entry().key().asByteBuffer().get(), actual.entry().key().asByteBuffer().get());
        if (expected.entry().committedValue() == null) {
            assertNull(actual.entry().committedValue());
        } else {
            assertEquals(expected.entry().committedValue().byteSize(), actual.entry().committedValue().byteSize());
            assertEquals(expected.entry().committedValue().asByteBuffer().get(), actual.entry().committedValue().asByteBuffer().get());
        }
        assertEquals(expected.logId(), actual.logId());
    }
}
