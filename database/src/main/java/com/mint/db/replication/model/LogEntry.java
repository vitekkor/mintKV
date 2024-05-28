package com.mint.db.replication.model;

import com.mint.db.dao.Entry;
import com.mint.db.raft.model.LogId;
import com.mint.db.replication.model.impl.OperationType;

public interface LogEntry<D> {
    OperationType operationType();

    Entry<D> entry();

    LogId logId();
}
