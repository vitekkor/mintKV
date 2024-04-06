package com.mint.db.impl;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class MemorySegmentComparator {

    public static int compare(MemorySegment o1, MemorySegment o2) {
        long offset = MemorySegment.mismatch(o1, 0, o1.byteSize(), o2, 0, o2.byteSize());
        if (offset == -1 || offset == o1.byteSize() || offset == o2.byteSize()) {
            return (int) offset;
        }
        return Byte.compare(o1.get(ValueLayout.JAVA_BYTE, offset), o2.get(ValueLayout.JAVA_BYTE, offset));
    }
}