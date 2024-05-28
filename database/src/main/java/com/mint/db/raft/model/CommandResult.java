package com.mint.db.raft.model;

public sealed interface CommandResult permits GetCommandResult, InsertCommandResult {
    /**
     * Current term for the receiving process to update itself.
     */
    long term();

    /**
     * Key
     *
     * @return entry key
     */
    String key();

    /**
     * Value. null if it {@link InsertCommand}
     *
     * @return entry value
     */
    String value();
}
