package com.mint.db.http.server;

import com.mint.db.raft.model.Command;
import com.mint.db.raft.model.CommandResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

public class CallbackKeeper {
    private static final Logger logger = LoggerFactory.getLogger(CallbackKeeper.class);

    private final Map<Command, BiConsumer<Command, CommandResult>> commandBiConsumerConcurrentHashMap
            = new ConcurrentHashMap<>();

    public void addClientCommandCallback(Command command, BiConsumer<Command, CommandResult> callback) {
        logger.info("Add callback on command {}", command);
        commandBiConsumerConcurrentHashMap.put(command, callback);
    }

    public void onClientCommandResult(Command command, CommandResult commandResult) {
        logger.info("Callback on command {} with result {}", command, commandResult);
        BiConsumer<Command, CommandResult> callback = commandBiConsumerConcurrentHashMap.remove(command);
        if (callback != null) {
            callback.accept(command, commandResult);
        } else {
            logger.warn("Callback on command {} is empty", command);
        }
    }
}
