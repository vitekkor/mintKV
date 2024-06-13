package com.mint.db.raft.integration.configuration;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.mint.db.config.NodeConfig;
import com.mint.db.config.annotations.DaoBean;
import com.mint.db.config.annotations.EnvironmentBean;
import com.mint.db.config.annotations.NodeConfiguration;
import com.mint.db.config.annotations.ReplicatedLogManagerBean;
import com.mint.db.dao.Dao;
import com.mint.db.dao.Entry;
import com.mint.db.raft.Environment;
import com.mint.db.raft.EnvironmentImpl;
import com.mint.db.raft.StateMachine;
import com.mint.db.raft.integration.configuration.annotations.TestStateMachineBean;
import com.mint.db.raft.integration.system.DistributedTestSystem;
import com.mint.db.raft.integration.system.ITStateMachine;
import com.mint.db.replication.ReplicatedLogManager;
import jakarta.inject.Singleton;

import java.lang.foreign.MemorySegment;

public class IntegrationTestInjectionModule extends AbstractModule {
    private final DistributedTestSystem testSystem;
    private final int nodeId;

    @Provides
    @TestStateMachineBean
    @Singleton
    StateMachine<MemorySegment> provideStateMachine(
            @DaoBean Dao<MemorySegment, Entry<MemorySegment>> dao
    ) {
        return new ITStateMachine(dao, testSystem, nodeId);
    }

    @Provides
    @EnvironmentBean
    @Singleton
    static Environment<MemorySegment> provideEnvironment(
            @NodeConfiguration NodeConfig nodeConfig,
            @ReplicatedLogManagerBean ReplicatedLogManager<MemorySegment> replicatedLogManager,
            @TestStateMachineBean StateMachine<MemorySegment> stateMachine
    ) {
        return new EnvironmentImpl(nodeConfig, replicatedLogManager, stateMachine);
    }

    public IntegrationTestInjectionModule(DistributedTestSystem testSystem, int nodeId) {
        this.testSystem = testSystem;
        this.nodeId = nodeId;
    }

    @Override
    protected void configure() {
        /*var dao = new BaseDao();
        bind(Dao.class).annotatedWith(TestDaoBean.class).toInstance(dao);
        bind(StateMachine.class).annotatedWith(TestStateMachineBean.class)
                .toInstance(new ITStateMachine(dao, testSystem, nodeId));
        super.configure();*/
    }
}
