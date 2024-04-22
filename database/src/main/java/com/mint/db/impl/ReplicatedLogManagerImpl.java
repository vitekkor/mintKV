package com.mint.db.impl;

import com.mint.db.LogEntry;
import com.mint.db.ReplicatedLogManager;
import com.mint.db.config.NodeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;


public class ReplicatedLogManagerImpl implements ReplicatedLogManager<MemorySegment> {
    private static final Logger log = LoggerFactory.getLogger(ReplicatedLogManagerImpl.class);
    private final NodeConfig nodeConfig;
    private final Path logFile;

    public ReplicatedLogManagerImpl(NodeConfig nodeConfig) {
        this.nodeConfig = nodeConfig;
        logFile = createLogFile();
    }

    @Override
    public synchronized void appendLogEntry(LogEntry<MemorySegment> logEntry) {
        try (FileChannel fileChannel = FileChannel.open(
                logFile,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND)
        ) {
            int written = fileChannel.write(serializeLogEntry(logEntry));
            if (written != getLogEntrySize(logEntry)) {
                throw new RuntimeException("Failed to write log entry");
            }
            log.info("Appended log entry: {}, written {} bytes", logEntry, written);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private ByteBuffer serializeLogEntry(LogEntry<MemorySegment> logEntry) {
        ByteBuffer buffer = ByteBuffer.allocate((int) getLogEntrySize(logEntry));
        int offset = 0;
        buffer.putLong(offset, logEntry.operationType().getValue());
        offset += Long.BYTES;
        buffer.putLong(offset, logEntry.entry().key().byteSize());
        offset += Long.BYTES;
        buffer.put(offset, logEntry.entry().key().toArray(ValueLayout.OfByte.JAVA_BYTE));
        offset += logEntry.entry().key().byteSize();
        buffer.putLong(offset, logEntry.entry().value().byteSize());
        offset += Long.BYTES;
        if (logEntry.entry().value() != null) {
            buffer.put(offset, logEntry.entry().value().toArray(ValueLayout.OfByte.JAVA_BYTE));
            offset += logEntry.entry().value().byteSize();
        } else {
            buffer.putLong(offset, -1);
            offset += Long.BYTES;
        }
        buffer.putLong(offset, logEntry.timestamp());
        return buffer;
    }

    private long getLogEntrySize(LogEntry<MemorySegment> logEntry) {
        return Long.BYTES
                + Long.BYTES
                + logEntry.entry().key().byteSize()
                + Long.BYTES
                + (logEntry.entry().value() == null ? 0 : logEntry.entry().value().byteSize())
                + Long.BYTES;
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
}
