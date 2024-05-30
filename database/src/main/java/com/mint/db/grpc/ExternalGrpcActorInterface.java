package com.mint.db.grpc;

import com.mint.db.raft.model.Command;
import com.mint.db.raft.model.CommandResult;

import java.util.function.BiConsumer;

public interface ExternalGrpcActorInterface {
    /**
     * Sends the {@code command} message to the process {@code destId} (from 1 to [nProcesses]).
     * Calls {@code onCommandResult} on result.
     *
     * @param onCommandResult int srcId, CommandResult onCommandResult
     */
    void sendClientCommand(
            int destId,
            Command command,
            BiConsumer<Integer, CommandResult> onCommandResult
    );

    /**
     * Sends the {@code commandResult} to the client.
     *
     * @param commandResult result of applying client command
     */
    void onClientCommandResult(
            Command command,
            CommandResult commandResult
    );
}
