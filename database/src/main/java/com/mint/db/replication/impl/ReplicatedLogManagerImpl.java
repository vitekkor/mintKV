package com.mint.db.replication.impl;

import com.mint.db.config.NodeConfig;
import com.mint.db.raft.model.LogId;
import com.mint.db.replication.ReplicatedLogManager;
import com.mint.db.replication.model.LogEntry;
import com.mint.db.replication.model.PersistentState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


public class ReplicatedLogManagerImpl implements ReplicatedLogManager<MemorySegment>, Closeable {
    private static final Logger log = LoggerFactory.getLogger(ReplicatedLogManagerImpl.class);
    private static final int BUFFER_SIZE = 64 * 1024;
    private static final int BLOB_BUFFER_SIZE = 512;
    private final NodeConfig nodeConfig;
    private final Path logFile;
    private final OutputStream outputStream;
    private final ByteArraySegment longBuffer = new ByteArraySegment(Long.BYTES);
    private final ByteArraySegment blobBuffer = new ByteArraySegment(BLOB_BUFFER_SIZE);

    private PersistentState state;
    private long commitIndex = 0; // todo read from file

    public ReplicatedLogManagerImpl(NodeConfig nodeConfig, PersistentState state) {
        this.nodeConfig = nodeConfig;
        this.state = state;
        logFile = createLogFile();
        try {
            outputStream = new BufferedOutputStream(
                    new FileOutputStream(logFile.toFile()),
                    BUFFER_SIZE
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to open log file", e);
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
        try {
            serializeLogEntry(logEntry);
            outputStream.flush();
            log.info("Appended log entry: {}", logEntry);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write log entry", e);
        }
    }

    @Override
    public LogId readLastLogId() {
        throw new RuntimeException("Not implemented"); // TODO
    }

    @Override
    public LogEntry<MemorySegment> readLog(long index) {
        throw new RuntimeException("Not implemented"); // TODO
    }

    @Override
    public long commitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        // todo write to file
        this.commitIndex = commitIndex;
    }

    private void serializeLogEntry(LogEntry<MemorySegment> logEntry) throws IOException {
        writeLong(logEntry.operationType().getValue(), outputStream);
        writeLong(logEntry.entry().key().byteSize(), outputStream);
        writeSegment(logEntry.entry().key(), outputStream);
        if (logEntry.entry().value() != null) {
            writeLong(logEntry.entry().value().byteSize(), outputStream);
            writeSegment(logEntry.entry().value(), outputStream);
        } else {
            writeLong(-1, outputStream);
        }
        writeLong(logEntry.logId().index(), outputStream);
        writeLong(logEntry.logId().term(), outputStream);
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
        while (true) {
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
    }

    public Path getLogFile() {
        return logFile;
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }
}
