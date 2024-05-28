package com.mint.db.raft;

/**
 * A type of the timeout in the Raft algorithm.
 */
public enum Timeout {
    /**
     * A randomized timeout when waiting for a heartbeat from a leader.
     */
    ELECTION_TIMEOUT,

    /**
     * A fixed timeout at which the leader shall send heatbeats.
     */
    LEADER_HEARTBEAT_PERIOD
}
