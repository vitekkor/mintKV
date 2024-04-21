package com.mint.db.config;

import java.util.List;

public final class NodeConfig {
    private int port;
    private int nodeId;
    private List<String> cluster;

    public NodeConfig() {

    }

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

    public void setPort(int port) {
        this.port = port;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public void setCluster(List<String> cluster) {
        this.cluster = cluster;
    }
}
