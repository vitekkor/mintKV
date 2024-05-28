package com.mint.db.raft;

/**
 * Environment interface for communication with other processes.
 */
public interface Environment {
    /**
     * Identifier of this process (from 1 to nProcesses).
     */
    int getProcessId();

    /**
     * The total number of processes in the system.
     */
    int getNProcesses();

    /**
     * Starts/restart timeout of a given Timeout type. When timeout elapses, Process.onTimeout is called.
     */
    void startTimeout(Timeout timeout);

    /**
     * Must be called on the client that has originally requested the command to be executed with the
     * result of the command's execution.
     */
    void onClientCommandResult(CommandResult result);

    /**
     * Reference to the persistent storage.
     */
    Storage getStorage();

    /**
     * Reference to the state machine.
     */
    StateMachine getMachine();
}

