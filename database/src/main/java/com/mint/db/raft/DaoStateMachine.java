package com.mint.db.raft;

import com.mint.db.dao.Dao;
import com.mint.db.dao.Entry;
import com.mint.db.raft.model.Command;
import com.mint.db.raft.model.CommandResult;

public class DaoStateMachine<K, V> implements StateMachine {
    private final Dao<K, ? extends Entry<V>> dao;

    public DaoStateMachine(Dao<K, ? extends Entry<V>> dao) {
        this.dao = dao;
    }

    @Override
    public CommandResult apply(Command command) {
        // TODO implement
        return null;
    }
}
