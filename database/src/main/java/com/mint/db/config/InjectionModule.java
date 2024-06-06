package com.mint.db.config;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.mint.db.config.annotations.*;
import com.mint.db.grpc.ExternalGrpcActorInterface;
import com.mint.db.grpc.InternalGrpcActor;
import com.mint.db.grpc.InternalGrpcActorInterface;
import com.mint.db.grpc.client.ExternalGrpcClient;
import com.mint.db.grpc.client.InternalGrpcClient;
import com.mint.db.grpc.server.ExternalServiceImpl;
import com.mint.db.raft.RaftActor;
import com.mint.db.raft.RaftActorInterface;
import com.mint.db.replication.model.PersistentState;

import java.io.FileNotFoundException;
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
            @RaftActorBean RaftActor raftActor
    ) {
        return new ExternalServiceImpl(nodeConfig, raftActor);
    }


    @Provides
    @RaftActorBean
    static RaftActorInterface provideRaftActorInterface(
            @InternalGrpcActorBean InternalGrpcActorInterface internalGrpcActor,
            @NodeConfiguration NodeConfig nodeConfig,
            @PersistentStateBean PersistentState persistentState,
            @ExternalGrpcActorBean ExternalGrpcActorInterface externalGrpcActor

    ) {
        return new RaftActor(internalGrpcActor, nodeConfig, persistentState, externalGrpcActor);
    }

    @Provides
    @InternalClientsBean
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

    @Provides
    @ExternalClientsBean
    static Map<Integer, ExternalGrpcClient> provideExternalGrpcClients(@NodeConfiguration NodeConfig nodeConfig) {
        Map<Integer, ExternalGrpcClient> externalGrpcClients = new HashMap<>();
        for (int nodeId = 0; nodeId < nodeConfig.getCluster().size(); nodeId++) {
            if (nodeId != nodeConfig.getNodeId()) {
                String nodeUrl = nodeConfig.getCluster().get(nodeId);
                externalGrpcClients.put(nodeId, new ExternalGrpcClient(nodeUrl));
            }
        }
        return externalGrpcClients;
    }
}
