package com.mint.db.config;

import java.util.List;

public final class NodeConfig {
    private int port;
    private int nodeId;
    private String logDir;
    private List<String> cluster;

    private NodeConfig() {

    }

    public NodeConfig(int port, int nodeId, String logDir, List<String> cluster) {
        this.port = port;
        this.nodeId = nodeId;
        this.cluster = cluster;
        this.logDir = logDir;
    }

    public int getPort() {
        return port;
    }

    public int getNodeId() {
        return nodeId;
    }

    public String getLogDir() {
        return logDir;
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

    public void setLogDir(String logDir) {
        this.logDir = logDir;
    }
}
