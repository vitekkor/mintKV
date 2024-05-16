package com.mint.db.dao.impl;

import com.mint.db.dao.Entry;
import com.mint.db.Raft;

import java.lang.foreign.MemorySegment;

public record BaseEntry<D>(D key, D value) implements Entry<D> {
    public static BaseEntry<MemorySegment> valueOf(Raft.LogEntry entry) {
        return new BaseEntry<>(
                StringDaoWrapper.toMemorySegment(entry.getKey().toStringUtf8()),
                StringDaoWrapper.toMemorySegment(entry.getValue().toStringUtf8())
        );
    }

    @Override
    public String toString() {
        return STR."{ key=\{key}, value=\{value} }";
    }
}
