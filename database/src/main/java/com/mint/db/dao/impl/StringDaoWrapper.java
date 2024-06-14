package com.mint.db.dao.impl;

import com.mint.db.dao.Dao;
import com.mint.db.dao.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class StringDaoWrapper implements Dao<String, Entry<String>> {
    Dao<MemorySegment, Entry<MemorySegment>> delegate;

    public StringDaoWrapper() {
        this.delegate = new BaseDao();
    }

    public static String toString(MemorySegment memorySegment) {
        if (memorySegment == null) {
            return null;
        }
        return new String(memorySegment.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
    }

    public static MemorySegment toMemorySegment(String string) {
        if (string == null) {
            return null;
        }
        return MemorySegment.ofArray(string.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Entry<String> get(String key) {
        Entry<MemorySegment> entry = delegate.get(toMemorySegment(key));
        if (entry == null) {
            return null;
        }

        return toBaseEntryString(entry);
    }

    @Override
    public Iterator<Entry<String>> get(String from, String to) {
        Iterator<Entry<MemorySegment>> iterator = delegate.get(toMemorySegment(from), toMemorySegment(to));
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Entry<String> next() {
                Entry<MemorySegment> next = iterator.next();
                return toBaseEntryString(next);
            }
        };
    }

    @Override
    public Entry<String> upsert(Entry<String> entry) {
        Entry<MemorySegment> delegateEntry = toBaseEntryMemorySegment(entry);

        Entry<MemorySegment> oldEntry = delegate.upsert(delegateEntry);
        return oldEntry != null ? toBaseEntryString(oldEntry) : null;
    }

    @Override
    public void remove(Entry<String> entry) {
        Entry<MemorySegment> delegateEntry = toBaseEntryMemorySegment(entry);
        delegate.remove(delegateEntry);
    }

    public static StringEntry toBaseEntryString(Entry<MemorySegment> entry) {
        return new StringEntry(
                toString(entry.key()),
                toString(entry.committedValue()),
                toString(entry.uncommittedValue()),
                entry.uncommittedValueIsNotNull()
        );
    }

    public static BaseEntry toBaseEntryMemorySegment(Entry<String> entry) {
        return new BaseEntry(
                toMemorySegment(entry.key()),
                toMemorySegment(entry.committedValue()),
                toMemorySegment(entry.uncommittedValue()),
                entry.uncommittedValueIsNotNull()
        );
    }

}
