package com.mint.db.raft.model;

import com.mint.DatabaseServiceOuterClass;

/**
 * Get command performed by the client on [StateMachine].
 *
 * @param processId Process that originally issued a command is expecting to get the result.
 * @param key       Key
 * @param readMode  READ_COMMITTED (from state machine), READ_LOCAL (from log) or READ_CONSENSUS (replicate reading)
 */
public record GetCommand(
        int processId,
        String key,
        DatabaseServiceOuterClass.ReadMode readMode
) implements Command {
    @Override
    public String value() {
        return null;
    }
}
