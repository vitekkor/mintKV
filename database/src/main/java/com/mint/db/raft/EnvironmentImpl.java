package com.mint.db.raft;

import com.mint.db.config.NodeConfig;
import com.mint.db.replication.ReplicatedLogManager;

import java.lang.foreign.MemorySegment;

public class EnvironmentImpl implements Environment<MemorySegment> {
    private final NodeConfig nodeConfig;
    private final ReplicatedLogManager<MemorySegment> replicatedLogManager;
    private final StateMachine<MemorySegment> memorySegmentStateMachine;

    public EnvironmentImpl(
            NodeConfig nodeConfig,
            ReplicatedLogManager<MemorySegment> replicatedLogManager,
            StateMachine<MemorySegment> memorySegmentStateMachine
    ) {
        this.nodeConfig = nodeConfig;
        this.replicatedLogManager = replicatedLogManager;
        this.memorySegmentStateMachine = memorySegmentStateMachine;
    }

    @Override
    public NodeConfig config() {
        return nodeConfig;
    }

    @Override
    public ReplicatedLogManager<MemorySegment> replicatedLogManager() {
        return replicatedLogManager;
    }

    @Override
    public StateMachine<MemorySegment> stateMachine() {
        return memorySegmentStateMachine;
    }
}
