package com.mint.db.raft.model;

/**
 * Unique identifier for a log entry.
 *
 * @param index index of the log entry, starting from 1
 * @param term  Term of the log entry
 */
public record LogId(long index, long term) {
}
