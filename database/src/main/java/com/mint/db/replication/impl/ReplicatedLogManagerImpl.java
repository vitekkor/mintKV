package com.mint.db.replication.impl;

import com.mint.db.config.NodeConfig;
import com.mint.db.dao.Dao;
import com.mint.db.dao.Entry;
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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


public class ReplicatedLogManagerImpl implements ReplicatedLogManager<MemorySegment> {
    private static final Logger log = LoggerFactory.getLogger(ReplicatedLogManagerImpl.class);
    private static final int BLOB_BUFFER_SIZE = 512;
    private static final int BUFFER_SIZE = 64 * 1024;
    private final Dao<MemorySegment, Entry<MemorySegment>> dao;
    private final NodeConfig nodeConfig;
    private final ByteArraySegment longBuffer = new ByteArraySegment(Long.BYTES);
    private final ByteArraySegment blobBuffer = new ByteArraySegment(BLOB_BUFFER_SIZE);
    private volatile long commitIndex = 0; // todo read from file
    private volatile Arena arena;
    private Path logFile;
    private Path indexFile;
    private volatile PersistentState state;
    private final AtomicLong lastLogOffset = new AtomicLong();
    private volatile long lastAppliedIndex = 0;
    private volatile OutputStream logOutputStream;
    private volatile OutputStream indexOutputStream;
    private FileChannel logOutputFileChannel;
    private FileChannel indexOutputFileChannel;
    private volatile MemorySegment logOutputMemorySegment;
    private volatile MemorySegment indexOutputMemorySegment;
    private volatile LogId lastLogId = new LogId(0, 0);

    public ReplicatedLogManagerImpl(
            NodeConfig nodeConfig,
            PersistentState state,
            Dao<MemorySegment, Entry<MemorySegment>> dao
    ) {
        this.dao = dao;
        this.nodeConfig = nodeConfig;
        this.state = state;
        initializeLogFiles();
        initializeOutputFileChannels();
        initializeMemorySegments();
        configureParameters();
    }

    public static LogEntry<MemorySegment> createLogEntry(
            OperationType operationType,
            MemorySegment key,
            MemorySegment committedValue,
            MemorySegment uncommittedValue,
            long index,
            long term,
            int processId
    ) {
        return new BaseLogEntry<>(
                operationType,
                new BaseEntry(key, committedValue, uncommittedValue, uncommittedValue != null),
                new LogId(index, term),
                processId
        );
    }

    private static long calculateLogEntrySize(LogEntry<MemorySegment> logEntry) {
        long size = Long.BYTES; // operation type
        size += Long.BYTES; // key size
        size += logEntry.entry().key().byteSize(); // key
        size += Long.BYTES; // committed value size
        if (logEntry.entry().committedValue() != null) {
            size += logEntry.entry().committedValue().byteSize(); // committed value
        }
        size += Long.BYTES; // uncommitted value size
        if (logEntry.entry().uncommittedValueIsNotNull()) {
            size += logEntry.entry().uncommittedValue().byteSize(); // uncommitted value
        }
        size += Long.BYTES; // log index
        size += Long.BYTES; // log term
        size += Long.BYTES; // processId
        return size;
    }

    private void initializeOutputFileChannels() {
        try {
            logOutputFileChannel = FileChannel.open(logFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
            indexOutputFileChannel = FileChannel.open(indexFile, StandardOpenOption.READ, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new RuntimeException("Failed to map fileChannels to files", e);
        }
    }

    private void initializeMemorySegments() {
        if (arena == null || !arena.scope().isAlive()) {
            arena = Arena.ofShared();
        }
        updateLogMemorySegment();
        updateIndexMemorySegment();
    }

    private void updateIndexMemorySegment() {
        if (arena == null || !arena.scope().isAlive()) {
            throw new RuntimeException("Arena is not alive");
        }
        try {
            indexOutputMemorySegment = indexOutputFileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    Files.size(indexFile), arena
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to map memorySegments to files", e);
        }
    }

    private void updateLogMemorySegment() {
        if (arena == null || !arena.scope().isAlive()) {
            throw new RuntimeException("Arena is not alive");
        }
        try {
            logOutputMemorySegment = logOutputFileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    Files.size(logFile),
                    arena
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to map memorySegments to files", e);
        }
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
            var entrySize = calculateLogEntrySize(logEntry);
            long entryOffset = lastLogOffset.getAndAdd(entrySize);
            writeLong(entryOffset, indexOutputStream);
            indexOutputStream.flush();
            lastLogId = logEntry.logId();
            lastAppliedIndex = logEntry.logId().index();
            log.info("Appended log entry: {}", logEntry);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write log entry", e);
        }
    }

    private void rollbackLog(long index) {
        try {
            log.debug("Rollback log from {}", index);
            closeOutputStreams();
            updateIndexMemorySegment();
            long offset = indexOutputMemorySegment.get(
                    ValueLayout.OfByte.JAVA_LONG_UNALIGNED,
                    index * Long.BYTES
            );
            rollbackEntries(deserializeLogEntries(offset));
            arena.close();
            logOutputFileChannel.truncate(offset);
            indexOutputFileChannel.truncate(index * Long.BYTES);
            lastLogOffset.set(offset);
            lastAppliedIndex = index;
            initializeMemorySegments();
        } catch (IOException e) {
            throw new RuntimeException("Failed to rollback log", e);
        }

        openLogOutputStreams(true);
    }

    private void rollbackEntries(List<LogEntry<MemorySegment>> oldEntries) {
        for (LogEntry<MemorySegment> oldEntry : oldEntries.reversed()) {
            var newEntry = new BaseEntry(
                    oldEntry.entry().key(),
                    oldEntry.entry().committedValue(),
                    null,
                    false
            );
            dao.remove(newEntry);
        }
    }

    @Override
    public LogId readLastLogId() {
        return lastLogId;
    }

    @Override
    public List<LogEntry<MemorySegment>> readLog(long fromIndex, long toIndex) {
        updateIndexMemorySegment();
        if (fromIndex < 0) {
            return Collections.emptyList();
        }
        var indexOffset = fromIndex * Long.BYTES;
        if (indexOutputMemorySegment.byteSize() <= indexOffset) {
            return Collections.emptyList();
        }
        long offset = indexOutputMemorySegment.get(
                ValueLayout.OfByte.JAVA_LONG_UNALIGNED,
                fromIndex * Long.BYTES
        );
        return deserializeLogEntries(offset, toIndex - fromIndex, false);
    }

    public List<LogEntry<MemorySegment>> deserializeLogEntries(long fromOffset) {
        return deserializeLogEntries(fromOffset, 10, true);
    }

    public List<LogEntry<MemorySegment>> deserializeLogEntries(long fromOffset, long amount, boolean tillEnd) {
        //CHECKSTYLE.OFF: VariableDeclarationUsageDistanceCheck
        updateLogMemorySegment();
        List<LogEntry<MemorySegment>> logEntries = new ArrayList<>((int) amount);
        try {
            long offset = fromOffset;
            while (offset < Files.size(logFile) && (tillEnd || logEntries.size() < amount)) {
                long operationType = logOutputMemorySegment.get(ValueLayout.OfByte.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
                long keySize = logOutputMemorySegment.get(ValueLayout.OfByte.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
                MemorySegment key = logOutputMemorySegment.asSlice(offset, keySize);
                offset += keySize;
                long committedValueSize = logOutputMemorySegment.get(ValueLayout.OfByte.JAVA_LONG_UNALIGNED, offset);
                MemorySegment committedValue = null;
                if (committedValueSize == -1) {
                    offset += Long.BYTES;
                } else {
                    offset += Long.BYTES;
                    committedValue = logOutputMemorySegment.asSlice(offset, committedValueSize);
                    offset += committedValueSize;
                }
                long uncommittedValueSize = logOutputMemorySegment.get(ValueLayout.OfByte.JAVA_LONG_UNALIGNED, offset);
                MemorySegment uncommittedValue = null;
                if (uncommittedValueSize == -1) {
                    offset += Long.BYTES;
                } else {
                    offset += Long.BYTES;
                    uncommittedValue = logOutputMemorySegment.asSlice(offset, uncommittedValueSize);
                    offset += uncommittedValueSize;
                }
                long index = logOutputMemorySegment.get(ValueLayout.OfByte.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
                long term = logOutputMemorySegment.get(ValueLayout.OfByte.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
                long processId = logOutputMemorySegment.get(ValueLayout.OfByte.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
                logEntries.add(
                        createLogEntry(
                                OperationType.fromLong(operationType),
                                key,
                                committedValue,
                                uncommittedValue,
                                index,
                                term,
                                (int) processId
                        )
                );
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

    @Override
    public void setCommitIndex(long commitIndex) {
        // todo write to file
        this.commitIndex = commitIndex;
    }

    @Override
    public long lastAppliedIndex() {
        return lastAppliedIndex;
    }

    private void serializeLogEntry(LogEntry<MemorySegment> logEntry) throws IOException {
        writeLong(logEntry.operationType().getValue(), logOutputStream);
        writeLong(logEntry.entry().key().byteSize(), logOutputStream);
        writeSegment(logEntry.entry().key(), logOutputStream);
        if (logEntry.entry().committedValue() != null) {
            writeLong(logEntry.entry().committedValue().byteSize(), logOutputStream);
            writeSegment(logEntry.entry().committedValue(), logOutputStream);
        } else {
            writeLong(-1, logOutputStream);
        }
        if (logEntry.entry().uncommittedValueIsNotNull()) {
            writeLong(logEntry.entry().uncommittedValue().byteSize(), logOutputStream);
            writeSegment(logEntry.entry().uncommittedValue(), logOutputStream);
        } else {
            writeLong(-1, logOutputStream);
        }
        writeLong(logEntry.logId().index(), logOutputStream);
        writeLong(logEntry.logId().term(), logOutputStream);
        writeLong(logEntry.processId(), logOutputStream);
    }

    private void writeLong(final long value, final OutputStream os) throws IOException {
        longBuffer.segment().set(ValueLayout.OfLong.JAVA_LONG_UNALIGNED, 0, value);
        longBuffer.withArray(os::write);
    }

    private void writeSegment(final MemorySegment value, final OutputStream os) throws IOException {
        final long size = value.byteSize();
        blobBuffer.ensureCapacity(size);
        MemorySegment.copy(value, 0L, blobBuffer.segment(), 0L, size);
        blobBuffer.withArray(array -> os.write(array, 0, (int) size));
    }

    private void initializeLogFiles() {
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
        Path databaseFile = logDir.resolve("database");
        if (!Files.exists(databaseFile)) {
            try {
                Files.createFile(databaseFile);
            } catch (FileAlreadyExistsException e) {
                throw new RuntimeException("Database file already exists", e);
            } catch (Exception e) {
                throw new RuntimeException("Failed to create database file", e);
            }
        }
        try {
            if (Files.size(databaseFile) == 0) {
                logFile = createLogFile(logDir);
                indexFile = createIndexFile();
                var name = logFile.getFileName().toString();
                var nameWithoutExtensionWithDelimiter =
                        STR."\{name.substring(0, name.lastIndexOf('.'))}\n";
                Files.write(databaseFile, nameWithoutExtensionWithDelimiter.getBytes());
                openLogOutputStreams(false);
            } else {
                var nameWithDelimiter = Files.readString(databaseFile);
                var name = nameWithDelimiter.substring(0, nameWithDelimiter.length() - 1);
                logFile = logDir.resolve(STR."\{name}.log");
                indexFile = logDir.resolve(STR."\{name}.index");
                openLogOutputStreams(true);
            }
            if (!Files.exists(logFile) || !Files.exists(indexFile)) {
                throw new RuntimeException("Log file or index file does not exist");
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read database file", e);
        }
    }

    private void configureParameters() {
        // find last log offset in index file
        try {
            long lastLogOffset = Files.size(logFile);
            long lastLogIndex = Files.size(indexFile) / Long.BYTES;
            if (lastLogIndex > 0) {
                long lastEntryOffset = indexOutputMemorySegment.get(
                        ValueLayout.OfByte.JAVA_LONG_UNALIGNED,
                        (lastLogIndex - 1) * Long.BYTES
                );
                var lastEntry = deserializeLogEntries(lastEntryOffset, 1, false).getLast();
                this.lastLogId = lastEntry.logId();
                this.lastAppliedIndex = lastEntry.logId().index();
            }
            this.lastLogOffset.set(lastLogOffset);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read index file", e);
        }
    }

    private Path createLogFile(Path logDir) {
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
            String fileName = logFile.getFileName().toString();
            String fileNameWithoutExtension = fileName.substring(0, fileName.lastIndexOf('.'));
            indexFile = logFile.resolveSibling(STR."\{fileNameWithoutExtension}.index");
            Files.createFile(indexFile);
        } catch (InvalidPathException | IOException e) {
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
            closeOutputStreams();
            if (arena.scope().isAlive()) {
                logOutputMemorySegment.force();
                indexOutputMemorySegment.force();
            }
            logOutputFileChannel.close();
            indexOutputFileChannel.close();
            if (arena.scope().isAlive()) {
                arena.close();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to close log file or index file", e);
        }
    }

    private void closeOutputStreams() {
        try {
            logOutputStream.flush();
            logOutputStream.close();

            indexOutputStream.flush();
            indexOutputStream.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to close log streams", e);
        }
    }

    private void openLogOutputStreams(boolean append) {
        try {
            logOutputStream = new BufferedOutputStream(new FileOutputStream(logFile.toFile(), append), BUFFER_SIZE);
            indexOutputStream = new BufferedOutputStream(new FileOutputStream(indexFile.toFile(), append), Long.BYTES);
        } catch (IOException e) {
            throw new RuntimeException("Failed to open log file or index file", e);
        }
    }
}
