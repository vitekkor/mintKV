package com.mint.db.config;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.mint.db.config.annotations.ExternalClientsBean;
import com.mint.db.config.annotations.InternalClientsBean;
import com.mint.db.config.annotations.InternalGrpcActorBean;
import com.mint.db.config.annotations.NodeConfiguration;
import com.mint.db.config.annotations.PersistentStateBean;
import com.mint.db.config.annotations.RaftActorBean;
import com.mint.db.grpc.InternalGrpcActor;
import com.mint.db.grpc.client.ExternalGrpcClient;
import com.mint.db.grpc.client.InternalGrpcClient;
import com.mint.db.raft.RaftActor;
import com.mint.db.replication.model.PersistentState;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

public class InjectionModule extends AbstractModule {
    @Provides
    @NodeConfiguration
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
    static InternalGrpcActor provideInternalGrpcActor(
            @InternalClientsBean Map<String, InternalGrpcClient> internalGrpcClients
    ) {
        return new InternalGrpcActor(internalGrpcClients);
    }

    @Provides
    @RaftActorBean
    static RaftActor provideRaftActor(
            @InternalGrpcActorBean InternalGrpcActor internalGrpcActor,
            @NodeConfiguration NodeConfig nodeConfig,
            @PersistentStateBean PersistentState persistentState
    ) {
        return new RaftActor(internalGrpcActor, nodeConfig, persistentState);
    }

    @Provides
    @InternalClientsBean
    static Map<String, InternalGrpcClient> provideInternalGrpcClients(@NodeConfiguration NodeConfig nodeConfig) {
        Map<String, InternalGrpcClient> internalGrpcClients = new HashMap<>();
        for (int nodeId = 0; nodeId < nodeConfig.getCluster().size(); nodeId++) {
            if (nodeId != nodeConfig.getNodeId()) {
                String nodeUrl = nodeConfig.getCluster().get(nodeId);
                internalGrpcClients.put(nodeUrl, new InternalGrpcClient(nodeUrl));
            }
        }
        return internalGrpcClients;
    }

    @Provides
    @ExternalClientsBean
    static Map<String, ExternalGrpcClient> provideExternalGrpcClients(@NodeConfiguration NodeConfig nodeConfig) {
        Map<String, ExternalGrpcClient> externalGrpcClients = new HashMap<>();
        for (int nodeId = 0; nodeId < nodeConfig.getCluster().size(); nodeId++) {
            if (nodeId != nodeConfig.getNodeId()) {
                String nodeUrl = nodeConfig.getCluster().get(nodeId);
                externalGrpcClients.put(nodeUrl, new ExternalGrpcClient(nodeUrl));
            }
        }
        return externalGrpcClients;
    }
}
