package com.mint.db.raft;

import com.mint.db.dao.Dao;
import com.mint.db.dao.Entry;
import com.mint.db.raft.model.CommandResult;
import com.mint.db.replication.model.LogEntry;

public class DaoStateMachine<D, E extends Entry<D>> implements StateMachine<D> {
    private final Dao<D, E> dao;

    public DaoStateMachine(Dao<D, E> dao) {
        this.dao = dao;
    }

    @Override
    public CommandResult apply(LogEntry<D> logEntry) {
        // TODO implement
        return null;
    }
}
