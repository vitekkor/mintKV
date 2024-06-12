package com.mint.db.config;

import java.util.List;

public final class NodeConfig {
    private int httpPort;
    private int port;
    private int nodeId;
    private String logDir;
    private long heartbeatTimeoutMs;
    private List<String> cluster;

    private NodeConfig() {

    }

    public NodeConfig(
            int httpPort,
            int port,
            int nodeId,
            String logDir,
            long heartbeatTimeoutMs,
            List<String> cluster
    ) {
        this.httpPort = httpPort;
        this.port = port;
        this.nodeId = nodeId;
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        this.cluster = cluster;
        this.logDir = logDir;
    }

    public int getPort() {
        return port;
    }

    public int getHttpPort() {
        return httpPort;
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

    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
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

    public NodeConfig copy() {
        return new NodeConfig(httpPort, port, nodeId, logDir, heartbeatTimeoutMs, cluster);
    }

    public void setHeartbeatTimeoutMs(long heartbeatTimeoutMs) {
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
    }

    public long getHeartbeatTimeoutMs() {
        return heartbeatTimeoutMs;
    }

    public boolean heartbeatRandom() {
        return true;
    }
}
