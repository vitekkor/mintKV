package com.mint.db.raft.model;

/**
 * Insert command (put/delete) performed by the client on [StateMachine].
 *
 * @param processId   Process that originally issued a command is expecting to get the result.
 * @param key         Key
 * @param value       Value to set for the [key]. null if it is a delete command
 * @param uncommitted true if client don't worry about replication
 */
public record InsertCommand(
        long processId,
        String key,
        String value,
        boolean uncommitted
) implements Command {
}
