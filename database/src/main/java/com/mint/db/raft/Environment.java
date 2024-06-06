package com.mint.db.raft;


import com.mint.db.config.NodeConfig;
import com.mint.db.replication.ReplicatedLogManager;

/**
 * Environment interface for communication with other processes.
 */
public interface Environment<D> {
    /**
     * The total number of processes in the system.
     */
    default int nProcesses() {
        return config().getCluster().size();
    }

    /**
     * Identifier of this process.
     */
    default int nodeId() {
        return config().getNodeId();
    }

    /**
     * Reference to the node config.
     */
    NodeConfig config();

    /**
     * Reference to the replicated log manager.
     */
    ReplicatedLogManager<D> replicatedLogManager();


    /**
     * Reference to the state machine.
     */
    StateMachine<D> stateMachine();
}
