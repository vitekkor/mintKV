package com.mint.db.replication.model.impl;

import com.mint.db.Entry;
import com.mint.db.replication.model.LogEntry;

public record BaseLogEntry<D>(OperationType operationType, Entry<D> entry, long timestamp) implements LogEntry<D> {
    @Override
    public String toString() {
        return STR."{ operationType=\{operationType}, entry=\{entry}, timestamp=\{timestamp} }";
    }
}
