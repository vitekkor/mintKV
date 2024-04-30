import com.mint.db.config.ConfigParser;
import com.mint.db.impl.BaseEntry;
import com.mint.db.impl.StringDaoWrapper;
import com.mint.db.replication.impl.ReplicatedLogManagerImpl;
import com.mint.db.replication.model.LogEntry;
import com.mint.db.replication.model.impl.BaseLogEntry;
import com.mint.db.replication.model.impl.OperationType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ReplicatedLogManagerTest {

    @Test
    @DisplayName("Test append log entry")
    public void testAppendLogEntry() throws IOException {
        ReplicatedLogManagerImpl logManager = new ReplicatedLogManagerImpl(ConfigParser.parseConfig());
        LogEntry<MemorySegment> logEntry = createLogEntry(
                OperationType.PUT.getValue(),
                StringDaoWrapper.toMemorySegment("key"),
                StringDaoWrapper.toMemorySegment("value"),
                System.currentTimeMillis()
        );
        logManager.appendLogEntry(logEntry);
        Path logFile = logManager.getLogFile();
        try (
                FileChannel fileChannel = FileChannel.open(
                        logFile,
                        StandardOpenOption.READ
                )
        ) {
            LogEntry<MemorySegment> deserializedLogEntry = deserializeLogEntry(fileChannel, logFile);
            checkLogEntry(logEntry, deserializedLogEntry);
        }
    }

    @Test
    @DisplayName("Test with null value")
    public void testAppendLogEntryWithNullValue() throws IOException {
        ReplicatedLogManagerImpl logManager = new ReplicatedLogManagerImpl(ConfigParser.parseConfig());
        LogEntry<MemorySegment> logEntry = createLogEntry(
                OperationType.PUT.getValue(),
                StringDaoWrapper.toMemorySegment("key"),
                null,
                System.currentTimeMillis()
        );
        logManager.appendLogEntry(logEntry);
        Path logFile = logManager.getLogFile();
        try (
                FileChannel fileChannel = FileChannel.open(
                        logFile,
                        StandardOpenOption.READ
                )
        ) {
            LogEntry<MemorySegment> deserializedLogEntry = deserializeLogEntry(fileChannel, logFile);
            assertEquals(logEntry.operationType(), deserializedLogEntry.operationType());
        }
    }

    @Test
    @DisplayName("Test with 2 values")
    public void testAppendLogEntryWithTwoValues() throws IOException {
        ReplicatedLogManagerImpl logManager = new ReplicatedLogManagerImpl(ConfigParser.parseConfig());
        LogEntry<MemorySegment> logEntry = createLogEntry(
                OperationType.PUT.getValue(),
                StringDaoWrapper.toMemorySegment("key"),
                StringDaoWrapper.toMemorySegment("value"),
                System.currentTimeMillis()
        );
        logManager.appendLogEntry(logEntry);
        LogEntry<MemorySegment> logEntry2 = createLogEntry(
                OperationType.PUT.getValue(),
                StringDaoWrapper.toMemorySegment("key2"),
                StringDaoWrapper.toMemorySegment("value2"),
                System.currentTimeMillis()
        );
        logManager.appendLogEntry(logEntry2);
        Path logFile = logManager.getLogFile();
        try (
                FileChannel fileChannel = FileChannel.open(
                        logFile,
                        StandardOpenOption.READ
                )
        ) {
            List<LogEntry<MemorySegment>> deserializedLogEntries = deserializeLogEntries(fileChannel, logFile);
            checkLogEntry(logEntry, deserializedLogEntries.get(0));
            checkLogEntry(logEntry2, deserializedLogEntries.get(1));
        }
    }

    private LogEntry<MemorySegment> deserializeLogEntry(FileChannel fileChannel, Path logFile) throws IOException {
        return deserializeLogEntries(fileChannel, logFile).getFirst();
    }

    private List<LogEntry<MemorySegment>> deserializeLogEntries(
            FileChannel fileChannel, Path logFile) throws IOException {
        List<LogEntry<MemorySegment>> logEntries = new ArrayList<>();
        int offset = 0;
        while (offset < Files.size(logFile)) {
            MemorySegment ms = fileChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    Files.size(logFile),
                    Arena.ofAuto()
            );
            offset += Long.BYTES;
            long keySize = ms.get(ValueLayout.OfByte.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            offset += keySize;
            long valueSize = ms.get(ValueLayout.OfByte.JAVA_LONG_UNALIGNED, offset);
            MemorySegment value = null;
            if (valueSize == -1) {
                offset += Long.BYTES;
            } else {
                offset += Long.BYTES;
                value = ms.asSlice(offset, valueSize);
                offset += valueSize;
            }
            long timestamp = ms.get(ValueLayout.OfByte.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            MemorySegment key = ms.asSlice(offset, keySize);
            long operationType = ms.get(ValueLayout.OfByte.JAVA_LONG_UNALIGNED, offset);
            logEntries.add(createLogEntry(operationType, key, value, timestamp));
        }
        return logEntries;
    }

    private LogEntry<MemorySegment> createLogEntry(
            long operationType, MemorySegment key, MemorySegment value, long timestamp) {
        return new BaseLogEntry<>(
                OperationType.values()[(int) operationType],
                new BaseEntry<>(key, value),
                timestamp
        );
    }

    private void checkLogEntry(LogEntry<MemorySegment> expected, LogEntry<MemorySegment> actual) {
        assertEquals(expected.operationType(), actual.operationType());
        assertEquals(expected.entry().key().byteSize(), actual.entry().key().byteSize());
        if (expected.entry().value() == null) {
            assertNull(actual.entry().value());
        } else {
            assertEquals(expected.entry().value().byteSize(), actual.entry().value().byteSize());
        }
        assertEquals(expected.timestamp(), actual.timestamp());
    }
}
