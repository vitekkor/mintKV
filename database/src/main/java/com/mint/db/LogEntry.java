package com.mint.db;

public interface LogEntry<D> {
    OperationType operationType();

    Entry<D> entry();

    long timestamp();
}
