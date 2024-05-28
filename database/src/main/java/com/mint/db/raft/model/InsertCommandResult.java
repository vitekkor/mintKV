package com.mint.db.raft.model;

public record InsertCommandResult(
        long term,
        String key
) implements CommandResult {
    @Override
    public String value() {
        return null;
    }
}
