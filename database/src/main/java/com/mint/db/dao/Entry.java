package com.mint.db.dao;

public interface Entry<D> {
    D key();

    D committedValue();

    D uncommittedValue();

    boolean uncommittedValueIsNotNull();

    default D readUncommittedValue() {
        return uncommittedValueIsNotNull() ? uncommittedValue() : committedValue();
    }

    int processId();
}
