package com.mint.db.raft.model;

public sealed interface Command permits GetCommand, InsertCommand {
    /**
     * Process that originally issued a command is expecting to get the result.
     *
     * @return process id
     */
    long processId();

    /**
     * Key.
     *
     * @return entry key
     */
    String key();

    /**
     * Value. null if it {@link GetCommand} or {@link InsertCommand} and it is a delete request.
     *
     * @return entry value
     */
    String value();
}
