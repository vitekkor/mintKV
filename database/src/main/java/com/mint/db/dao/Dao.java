package com.mint.db.dao;

import java.util.Iterator;

public interface Dao<D, E extends Entry<D>> {
    E get(D key);

    Iterator<E> get(D from, D to);

    E upsert(E entry);

    default Iterator<E> all() {
        return get(null, null);
    }

    void remove(E entry);
}