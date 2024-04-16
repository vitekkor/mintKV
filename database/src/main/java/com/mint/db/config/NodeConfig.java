package com.mint.db.config;

import java.util.List;

public final class NodeConfig {
    private final int port;
    private final int nodeId;
    private final List<String> cluster;

    public NodeConfig(int port, int nodeId, List<String> cluster) {
        this.port = port;
        this.nodeId = nodeId;
        this.cluster = cluster;
    }

    public int getPort() {
        return port;
    }

    public int getNodeId() {
        return nodeId;
    }

    public List<String> getCluster() {
        return cluster;
    }
}
