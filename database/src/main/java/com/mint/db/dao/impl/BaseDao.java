package com.mint.db.dao.impl;

import com.mint.db.dao.Dao;
import com.mint.db.dao.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class BaseDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> delegate =
            new ConcurrentSkipListMap<>(MemorySegmentComparator::compare);

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return delegate.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return delegate.values().iterator();
        } else if (to == null) {
            return delegate.tailMap(from).values().iterator();
        } else if (from == null) {
            return delegate.headMap(to).values().iterator();
        }
        return delegate.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> upsert(Entry<MemorySegment> entry) {
        return delegate.put(entry.key(), entry);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BaseDao baseDao)) return false;

        return delegate.equals(baseDao.delegate);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }
}
