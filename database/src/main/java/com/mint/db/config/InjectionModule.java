package com.mint.db.config;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.mint.db.config.annotations.CallbackKeeperBean;
import com.mint.db.config.annotations.DaoBean;
import com.mint.db.config.annotations.EnvironmentBean;
import com.mint.db.config.annotations.ExternalGrpcActorBean;
import com.mint.db.config.annotations.InternalClientsBean;
import com.mint.db.config.annotations.InternalGrpcActorBean;
import com.mint.db.config.annotations.NodeConfiguration;
import com.mint.db.config.annotations.PersistentStateBean;
import com.mint.db.config.annotations.RaftActorBean;
import com.mint.db.config.annotations.ReplicatedLogManagerBean;
import com.mint.db.config.annotations.StateMachineBean;
import com.mint.db.dao.Dao;
import com.mint.db.dao.Entry;
import com.mint.db.dao.impl.BaseDao;
import com.mint.db.grpc.InternalGrpcActor;
import com.mint.db.grpc.InternalGrpcActorInterface;
import com.mint.db.grpc.client.InternalGrpcClient;
import com.mint.db.grpc.server.ExternalServiceImpl;
import com.mint.db.http.server.CallbackKeeper;
import com.mint.db.raft.DaoStateMachine;
import com.mint.db.raft.Environment;
import com.mint.db.raft.EnvironmentImpl;
import com.mint.db.raft.RaftActor;
import com.mint.db.raft.StateMachine;
import com.mint.db.replication.ReplicatedLogManager;
import com.mint.db.replication.impl.ReplicatedLogManagerImpl;
import com.mint.db.replication.model.PersistentState;
import jakarta.inject.Singleton;

import java.io.FileNotFoundException;
import java.lang.foreign.MemorySegment;
import java.util.HashMap;
import java.util.Map;

public class InjectionModule extends AbstractModule {
    @Provides
    @PersistentStateBean
    static PersistentState provicePersistentState() {
        // TODO READ FROM FILE
        return new PersistentState();
    }

    @Provides
    @CallbackKeeperBean
    static CallbackKeeper provideCallbackKeeper() {
        return new CallbackKeeper();
    }

    @Provides
    @NodeConfiguration
    static NodeConfig proviceNodeConfig() throws FileNotFoundException {
        return ConfigParser.parseConfig();
    }

    @Provides
    @InternalGrpcActorBean
    static InternalGrpcActorInterface provideInternalGrpcActorInterface(
            @InternalClientsBean Map<Integer, InternalGrpcClient> internalGrpcClients
    ) {
        return new InternalGrpcActor(internalGrpcClients);
    }

    @Provides
    @ExternalGrpcActorBean
    static ExternalServiceImpl provideExternalGrpcActorInterface(
            @NodeConfiguration NodeConfig nodeConfig,
            @RaftActorBean RaftActor raftActor,
            @CallbackKeeperBean CallbackKeeper callbackKeeper
    ) {
        return new ExternalServiceImpl(nodeConfig, raftActor, callbackKeeper);
    }

    @Provides
    @DaoBean
    static Dao<MemorySegment, Entry<MemorySegment>> provideDao() {
        return new BaseDao();
    }

    @Provides
    @StateMachineBean
    static StateMachine<MemorySegment> provideStateMachine(
            @DaoBean Dao<MemorySegment, Entry<MemorySegment>> dao
    ) {
        return new DaoStateMachine(dao);
    }

    @Provides
    @ReplicatedLogManagerBean
    static ReplicatedLogManager<MemorySegment> provideReplicatedLogManager(
            @NodeConfiguration NodeConfig nodeConfig,
            @PersistentStateBean PersistentState persistentState,
            @DaoBean Dao<MemorySegment, Entry<MemorySegment>> dao
    ) {
        return new ReplicatedLogManagerImpl(nodeConfig, persistentState, dao);
    }

    @Provides
    @EnvironmentBean
    static Environment<MemorySegment> provideEnvironment(
            @NodeConfiguration NodeConfig nodeConfig,
            @ReplicatedLogManagerBean ReplicatedLogManager<MemorySegment> replicatedLogManager,
            @StateMachineBean StateMachine<MemorySegment> stateMachine
    ) {
        return new EnvironmentImpl(nodeConfig, replicatedLogManager, stateMachine);
    }

    @Provides
    @RaftActorBean
    @Singleton
    static RaftActor provideRaftActorInterface(
            @InternalGrpcActorBean InternalGrpcActorInterface internalGrpcActor,
            @EnvironmentBean Environment<MemorySegment> environment,
            @CallbackKeeperBean CallbackKeeper callbackKeeper
    ) {
        return new RaftActor(internalGrpcActor, environment, callbackKeeper);
    }

    @Provides
    @InternalClientsBean
    @Singleton
    static Map<Integer, InternalGrpcClient> provideInternalGrpcClients(@NodeConfiguration NodeConfig nodeConfig) {
        Map<Integer, InternalGrpcClient> internalGrpcClients = new HashMap<>();
        for (int nodeId = 0; nodeId < nodeConfig.getCluster().size(); nodeId++) {
            if (nodeId != nodeConfig.getNodeId()) {
                String nodeUrl = nodeConfig.getCluster().get(nodeId);
                internalGrpcClients.put(nodeId, new InternalGrpcClient(nodeUrl));
            }
        }
        return internalGrpcClients;
    }
}
