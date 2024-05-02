package com.mint.db.replication.impl;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

final class ByteArraySegment {
    private byte[] array;
    private MemorySegment segment;

    ByteArraySegment(final int capacity) {
        this.array = new byte[capacity];
        this.segment = MemorySegment.ofArray(array);
    }

    void withArray(final ArrayConsumer consumer) throws IOException {
        consumer.process(array);
    }

    MemorySegment segment() {
        return segment;
    }

    void ensureCapacity(final long size) {
        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Too big!");
        }

        final int capacity = (int) size;
        if (array.length >= capacity) {
            return;
        }

        // Grow to the nearest bigger power of 2
        final int newSize = Integer.highestOneBit(capacity) << 1;
        array = new byte[newSize];
        segment = MemorySegment.ofArray(array);
    }

    @FunctionalInterface
    interface ArrayConsumer {
        void process(byte[] array) throws IOException;
    }
}
