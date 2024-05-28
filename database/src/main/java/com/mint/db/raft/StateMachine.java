package com.mint.db.raft;

import com.mint.db.raft.model.Command;
import com.mint.db.raft.model.CommandResult;

public interface StateMachine {
    CommandResult apply(Command command);
}
