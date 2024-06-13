package com.mint.db.http.server;

import com.mint.db.raft.model.Command;
import com.mint.db.raft.model.CommandResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiConsumer;

public class CallbackKeeper {
    private static final Logger logger = LoggerFactory.getLogger(CallbackKeeper.class);

    private final Map<Command, ConcurrentLinkedQueue<BiConsumer<Command, CommandResult>>> commandBiConsumerConcurrentHashMap
            = new ConcurrentHashMap<>();

    public void addClientCommandCallback(Command command, BiConsumer<Command, CommandResult> callback) {
        logger.info("Add callback on command {}", command);
        commandBiConsumerConcurrentHashMap.compute(command, (k, v) -> {
            ConcurrentLinkedQueue<BiConsumer<Command, CommandResult>> queue =
                    (v == null) ? new ConcurrentLinkedQueue<>() : v;
            queue.add(callback);
            return queue;
        });
    }

    public void onClientCommandResult(Command command, CommandResult commandResult) {
        logger.info("Callback on command {} with result {}", command, commandResult);
        commandBiConsumerConcurrentHashMap.computeIfPresent(command, (k, queue) -> {
            BiConsumer<Command, CommandResult> callback = queue.poll();
            if (callback != null) {
                callback.accept(command, commandResult);
            } else {
                logger.warn("Callback on command {} is empty", command);
            }
            return queue.isEmpty() ? null : queue;
        });
    }
}
