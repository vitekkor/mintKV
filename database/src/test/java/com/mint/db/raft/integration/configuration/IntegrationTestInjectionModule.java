package com.mint.db.raft.integration.configuration;

import com.google.inject.AbstractModule;
import com.mint.db.dao.Dao;
import com.mint.db.dao.impl.BaseDao;
import com.mint.db.raft.StateMachine;
import com.mint.db.raft.integration.system.DistributedTestSystem;
import com.mint.db.raft.integration.system.ITStateMachine;

public class IntegrationTestInjectionModule extends AbstractModule {
    private final DistributedTestSystem testSystem;
    private final int nodeId;

    public IntegrationTestInjectionModule(DistributedTestSystem testSystem, int nodeId) {
        this.testSystem = testSystem;
        this.nodeId = nodeId;
    }

    @Override
    protected void configure() {
        var dao = new BaseDao();
        bind(Dao.class).toInstance(dao);
        bind(StateMachine.class).toInstance(new ITStateMachine(dao, testSystem, nodeId));
        super.configure();
    }
}
