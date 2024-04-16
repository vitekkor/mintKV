package com.mint.db.config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public final class ConfigParser {
    private static final String NODE_CONFIG_PATH_DEFAULT = "database/src/main/resources/node-config.yaml";
    private static final String NODE_CONFIG_ENV_PROPERTY = "mint.config.location";

    private ConfigParser() {

    }

    public static NodeConfig parseConfig() throws IOException {
        String configPath = System.getProperty(NODE_CONFIG_ENV_PROPERTY);

        File file = new File(Objects.requireNonNullElse(configPath, NODE_CONFIG_PATH_DEFAULT));

        int port = 0;
        int nodeId = 0;
        List<String> cluster = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("port:")) {
                    port = Integer.parseInt(line.substring(line.indexOf(':') + 1).trim());
                } else if (line.startsWith("node-id:")) {
                    nodeId = Integer.parseInt(line.substring(line.indexOf(':') + 1).trim());
                } else if (line.startsWith("-")) {
                    cluster.add(line.substring(1).trim());
                }
            }
        }

        return new NodeConfig(port, nodeId, cluster);
    }
}
