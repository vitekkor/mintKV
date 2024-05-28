package com.mint.db.raft.model;

public record GetCommandResult(
        long term,
        String key,
        String value
) implements CommandResult{
}
