package com.mint.db.util;

import com.mint.db.raft.model.Command;
import com.mint.db.raft.model.CommandResult;

@FunctionalInterface
public interface ClientCommandResultConsumer {
    void accept(int srcId, Command command, CommandResult commandResult);
}
