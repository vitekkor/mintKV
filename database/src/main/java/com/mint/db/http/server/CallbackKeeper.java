package com.mint.db.http.server;

import com.mint.db.raft.model.Command;
import com.mint.db.raft.model.CommandResult;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

public class CallbackKeeper {

    private final Map<Command, BiConsumer<Command, CommandResult>> commandBiConsumerConcurrentHashMap
            = new ConcurrentHashMap<>();

    public void addClientCommandCallback(Command command, BiConsumer<Command, CommandResult> callback) {
        commandBiConsumerConcurrentHashMap.put(command, callback);
    }

    public void onClientCommandResult(Command command, CommandResult commandResult) {
        BiConsumer<Command, CommandResult> callback = commandBiConsumerConcurrentHashMap.remove(command);
        if (callback != null) {
            callback.accept(command, commandResult);
        }
    }

}
