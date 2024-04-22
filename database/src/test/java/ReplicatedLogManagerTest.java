import com.mint.db.LogEntry;
import com.mint.db.OperationType;
import com.mint.db.config.ConfigParser;
import com.mint.db.impl.BaseEntry;
import com.mint.db.impl.BaseLogEntry;
import com.mint.db.impl.ReplicatedLogManagerImpl;
import com.mint.db.impl.StringDaoWrapper;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ReplicatedLogManagerTest {

    @Test
    public void testAppendLogEntry() throws FileNotFoundException, IOException {
        ReplicatedLogManagerImpl logManager = new ReplicatedLogManagerImpl(ConfigParser.parseConfig());
        LogEntry<MemorySegment> logEntry = new BaseLogEntry<>(
                OperationType.PUT,
                new BaseEntry<>(
                        StringDaoWrapper.toMemorySegment("key"),
                        StringDaoWrapper.toMemorySegment("value")
                ),
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
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(logFile));
            int offset = 0;
            long operationType = buffer.getLong(offset);
            offset += Long.BYTES;
            long keySize = buffer.getLong(offset);
            offset += Long.BYTES;
            byte[] key = new byte[(int) keySize];
            buffer.get(offset, key, 0, (int) keySize);
            offset += keySize;
            long valueSize = buffer.getLong(offset);
            offset += Long.BYTES;
            byte[] value = new byte[(int) valueSize];
            buffer.get(offset, value, 0, (int) valueSize);
            assertEquals(OperationType.PUT.getValue(), operationType);
            assertEquals("key", StringDaoWrapper.toString(MemorySegment.ofArray(key)));
            assertEquals("value", StringDaoWrapper.toString(MemorySegment.ofArray(value)));
        }
    }

}
