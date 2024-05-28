package com.mint.db.replication.model;

/**
 * Persistent state of a Raft process.
 *
 * @param currentTerm Latest term server has seen (initialized to 0 on first boot, increases monotonically)
 * @param votedFor    CandidateId that received vote in current term (or `null` if none)
 */
public record PersistentState(
        long currentTerm,
        Integer votedFor
) {
    public PersistentState() {
        this(0, null);
    }

    public PersistentState(long currentTerm) {
        this(currentTerm, null);
    }
}
