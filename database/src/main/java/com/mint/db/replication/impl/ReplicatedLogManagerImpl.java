package com.mint.db.replication.impl;

import com.mint.db.config.NodeConfig;
import com.mint.db.dao.impl.BaseEntry;
import com.mint.db.raft.model.LogId;
import com.mint.db.replication.ReplicatedLogManager;
import com.mint.db.replication.model.LogEntry;
import com.mint.db.replication.model.PersistentState;
import com.mint.db.replication.model.impl.BaseLogEntry;
import com.mint.db.replication.model.impl.OperationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;


public class ReplicatedLogManagerImpl implements ReplicatedLogManager<MemorySegment>, Closeable {
    private static final Logger log = LoggerFactory.getLogger(ReplicatedLogManagerImpl.class);
    private static final int BUFFER_SIZE = 64 * 1024;
    private static final int BLOB_BUFFER_SIZE = 512;
    private final NodeConfig nodeConfig;
    private final Path logFile;
    private final Path indexFile;
    private final ByteArraySegment longBuffer = new ByteArraySegment(Long.BYTES);
    private final ByteArraySegment blobBuffer = new ByteArraySegment(BLOB_BUFFER_SIZE);
    private OutputStream logOutputStream;
    private OutputStream indexOutputStream;
    private long lastLogOffset = 0;
    private PersistentState state;
    private long commitIndex = 0; // todo read from file
    private LogId lastLogId;
    private long lastAppliedIndex = 0;

    public ReplicatedLogManagerImpl(NodeConfig nodeConfig, PersistentState state) {
        this.nodeConfig = nodeConfig;
        this.state = state;
        logFile = createLogFile();
        indexFile = createIndexFile();
        openLogOutputStreams(false);
    }

    public static LogEntry<MemorySegment> createLogEntry(
            OperationType operationType, MemorySegment key, MemorySegment value, long index, long term) {
        return new BaseLogEntry<>(
                operationType,
                new BaseEntry<>(key, value),
                new LogId(index, term)
        );
    }

    @Override
    public PersistentState readPersistentState() {
        return state;
    }

    @Override
    public void writePersistentState(PersistentState state) {
        // TODO SAVE IN FILE
        this.state = state;
    }

    @Override
    public synchronized void appendLogEntry(LogEntry<MemorySegment> logEntry) {
        if (logEntry.logId().index() < lastAppliedIndex) {
            // rollback log
            rollbackLog(logEntry.logId().index());
        }
        try {
            serializeLogEntry(logEntry);
            logOutputStream.flush();
            writeLong(lastLogOffset, indexOutputStream);
            indexOutputStream.flush();
            lastLogOffset += calculateLogEntrySize(logEntry);
            lastLogId = logEntry.logId();
            lastAppliedIndex = logEntry.logId().index();
            log.info("Appended log entry: {}", logEntry);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write log entry", e);
        }
    }

    private void rollbackLog(long index) {
        close();
        try (
                FileChannel indexChannel = FileChannel.open(indexFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
                FileChannel logChannel = FileChannel.open(logFile, StandardOpenOption.WRITE)
        ) {
            Arena indexFileArena = Arena.ofConfined();
            MemorySegment indexSegment = indexChannel.map(FileChannel.MapMode.READ_WRITE, 0, Files.size(indexFile), indexFileArena);
            long offset = indexSegment.get(ValueLayout.OfByte.JAVA_LONG_UNALIGNED, index * Long.BYTES);
            logChannel.truncate(offset);
            indexFileArena.close();
            indexChannel.truncate(index * Long.BYTES);
        } catch (IOException e) {
            throw new RuntimeException("Failed to rollback log", e);
        }

        openLogOutputStreams(true);
    }

    @Override
    public LogId readLastLogId() {
        return lastLogId;
    }

    @Override
    public List<LogEntry<MemorySegment>> readLog(long fromIndex, long toIndex) {
        try (FileChannel fileChannel = FileChannel.open(indexFile, StandardOpenOption.READ)) {
            MemorySegment ms = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(indexFile), Arena.ofAuto());
            long offset = ms.get(ValueLayout.OfByte.JAVA_LONG_UNALIGNED, fromIndex * Long.BYTES);
            return deserializeLogEntries(offset, toIndex - fromIndex);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<LogEntry<MemorySegment>> deserializeLogEntries(long fromOffset, long amount) {
        //CHECKSTYLE.OFF: VariableDeclarationUsageDistanceCheck
        List<LogEntry<MemorySegment>> logEntries = new ArrayList<>();
        try (
                FileChannel fileChannel = FileChannel.open(logFile, StandardOpenOption.READ)
        ) {
            long offset = fromOffset;
            MemorySegment ms = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(logFile), Arena.ofAuto());
            while (offset < Files.size(logFile) && logEntries.size() < amount) {
                long operationType = ms.get(ValueLayout.OfByte.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
                long keySize = ms.get(ValueLayout.OfByte.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
                MemorySegment key = ms.asSlice(offset, keySize);
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
                long index = ms.get(ValueLayout.OfByte.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
                long term = ms.get(ValueLayout.OfByte.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
                logEntries.add(createLogEntry(OperationType.fromLong(operationType), key, value, index, term));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return logEntries;
        //CHECKSTYLE.ON
    }

    @Override
    public long commitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        // todo write to file
        this.commitIndex = commitIndex;
    }

    private long calculateLogEntrySize(LogEntry<MemorySegment> logEntry) {
        long size = Long.BYTES; // operation type
        size += Long.BYTES; // key size
        size += logEntry.entry().key().byteSize(); // key
        size += Long.BYTES; // value size
        if (logEntry.entry().value() != null) {
            size += logEntry.entry().value().byteSize(); // value
        }
        size += Long.BYTES; // log index
        size += Long.BYTES; // log term
        return size;
    }

    private void serializeLogEntry(LogEntry<MemorySegment> logEntry) throws IOException {
        writeLong(logEntry.operationType().getValue(), logOutputStream);
        writeLong(logEntry.entry().key().byteSize(), logOutputStream);
        writeSegment(logEntry.entry().key(), logOutputStream);
        if (logEntry.entry().value() != null) {
            writeLong(logEntry.entry().value().byteSize(), logOutputStream);
            writeSegment(logEntry.entry().value(), logOutputStream);
        } else {
            writeLong(-1, logOutputStream);
        }
        writeLong(logEntry.logId().index(), logOutputStream);
        writeLong(logEntry.logId().term(), logOutputStream);
    }

    private void writeLong(
            final long value,
            final OutputStream os) throws IOException {
        longBuffer.segment().set(
                ValueLayout.OfLong.JAVA_LONG_UNALIGNED,
                0,
                value);
        longBuffer.withArray(os::write);
    }

    private void writeSegment(
            final MemorySegment value,
            final OutputStream os) throws IOException {
        final long size = value.byteSize();
        blobBuffer.ensureCapacity(size);
        MemorySegment.copy(
                value,
                0L,
                blobBuffer.segment(),
                0L,
                size);
        blobBuffer.withArray(array ->
                os.write(
                        array,
                        0,
                        (int) size)
        );
    }

    private Path createLogFile() {
        if (nodeConfig.getLogDir() == null) {
            throw new IllegalArgumentException("Log directory is not set in the configuration");
        }
        Path logDir = Paths.get(nodeConfig.getLogDir());
        if (!Files.exists(logDir)) {
            try {
                Files.createDirectory(logDir);
            } catch (FileAlreadyExistsException e) {
                throw new RuntimeException("Log directory already exists", e);
            } catch (Exception e) {
                throw new RuntimeException("Failed to create log directory", e);
            }
        }
        if (!Files.isDirectory(logDir)) {
            throw new IllegalArgumentException("Log directory path is not a directory");
        }
        for (int i = 0; i < 10; i++) {
            Path logFile = logDir.resolve("log-" + System.currentTimeMillis() + ".log");
            if (!Files.exists(logFile)) {
                try {
                    Files.createFile(logFile);
                } catch (FileAlreadyExistsException e) {
                    continue;
                } catch (Exception e) {
                    throw new RuntimeException("Failed to create log file", e);
                }
                return logFile;
            }
        }
        throw new RuntimeException("Failed to create log file after 10 attempts");
    }

    private Path createIndexFile() {
        Path indexFile;
        try {
            indexFile = logFile.resolveSibling(STR."\{logFile.getFileName()}.index");
        } catch (InvalidPathException e) {
            throw new RuntimeException("Failed to create index file", e);
        }
        return indexFile;
    }

    public Path getLogFile() {
        return logFile;
    }

    public Path getIndexFile() {
        return indexFile;
    }

    @Override
    public void close() {
        try {
            logOutputStream.close();
            indexOutputStream.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to close log file or index file", e);
        }
    }

    private void openLogOutputStreams(boolean append) {
        try {
            logOutputStream = new BufferedOutputStream(
                    new FileOutputStream(logFile.toFile(), append),
                    BUFFER_SIZE
            );
            indexOutputStream = new BufferedOutputStream(
                    new FileOutputStream(indexFile.toFile(), append),
                    Long.BYTES
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to open log file or index file", e);
        }
    }
}
