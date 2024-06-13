package com.mint.db.dao.impl;

import com.mint.db.Raft;
import com.mint.db.dao.Entry;

import java.lang.foreign.MemorySegment;

public record BaseEntry(
        MemorySegment key,
        MemorySegment committedValue,
        MemorySegment uncommittedValue,
        boolean uncommittedValueIsNotNull
) implements Entry<MemorySegment> {
    public static BaseEntry valueOf(Raft.LogEntry entry) {
        return new BaseEntry(
                StringDaoWrapper.toMemorySegment(entry.getKey().toStringUtf8()),
                StringDaoWrapper.toMemorySegment(entry.getValue().toStringUtf8()),
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BaseEntry baseEntry)) {
            return false;
        }

        if (uncommittedValueIsNotNull != baseEntry.uncommittedValueIsNotNull) {
            return false;
        }
        if (!MemorySegmentComparator.equals(key, baseEntry.key)) {
            return false;
        }
        if (!MemorySegmentComparator.equals(committedValue, baseEntry.committedValue)) {
            return false;
        }
        return MemorySegmentComparator.equals(uncommittedValue, baseEntry.uncommittedValue);
    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + (committedValue != null ? committedValue.hashCode() : 0);
        result = 31 * result + (uncommittedValue != null ? uncommittedValue.hashCode() : 0);
        result = 31 * result + (uncommittedValueIsNotNull ? 1 : 0);
        return result;
    }
}
