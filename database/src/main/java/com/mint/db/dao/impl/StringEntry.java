package com.mint.db.dao.impl;

import com.mint.db.Raft;
import com.mint.db.dao.Entry;

public record StringEntry(
        String key,
        String committedValue,
        String uncommittedValue,
        boolean uncommittedValueIsNotNull
) implements Entry<String> {
    public static StringEntry valueOf(Raft.LogEntry entry) {
        return new StringEntry(
                entry.getKey().toStringUtf8(),
                entry.getValue().toStringUtf8(),
                null,
                false
        );
    }

    @Override
    public String toString() {
        return "BaseEntry{"
                + "key=" + key
                + ", committedValue=" + committedValue
                + ", uncommittedValue=" + uncommittedValue
                + ", uncommittedValueIsNotNull=" + uncommittedValueIsNotNull
                + '}';
    }
}
