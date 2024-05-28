package com.mint.db.raft;

import com.mint.db.dao.Entry;
import com.mint.db.raft.model.Command;
import com.mint.db.raft.model.CommandResult;
import com.mint.db.replication.model.LogEntry;

public interface StateMachine<D> {
    CommandResult apply(LogEntry<D> logEntry);
}
