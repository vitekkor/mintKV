package com.mint.db.dao.impl;

import com.mint.db.dao.Entry;
import com.mint.db.replication.model.LogEntry;

import java.lang.foreign.MemorySegment;

public record BaseEntry<D>(D key, D committedValue, D uncommittedValue, boolean uncommittedValueIsNotNull) implements Entry<D> {
    public static BaseEntry<MemorySegment> valueOf(LogEntry<MemorySegment> logEntry) {
        return new BaseEntry<>(
                StringDaoWrapper.toMemorySegment(entry.getKey().toStringUtf8()),
                StringDaoWrapper.toMemorySegment(entry.getValue().toStringUtf8()),
                null,
                false
        );
    }

    @Override
    public D uncommittedValue() {
        return uncommittedValueIsNotNull ? uncommittedValue : committedValue;
    }

    @Override
    public String toString() {
        return "BaseEntry{" +
                "key=" + key +
                ", committedValue=" + committedValue +
                ", uncommittedValue=" + uncommittedValue +
                ", uncommittedValueIsNotNull=" + uncommittedValueIsNotNull +
                '}';
    }
}
