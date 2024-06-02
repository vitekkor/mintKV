package com.mint.db.raft;

import com.mint.db.raft.model.CommandResult;
import com.mint.db.replication.model.LogEntry;

import java.lang.foreign.MemorySegment;

public interface StateMachine<D> {

    CommandResult apply(LogEntry<D> logEntry, boolean committed);
}
