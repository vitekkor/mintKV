package com.mint.db.config;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.InputStream;
import java.util.Objects;

public final class ConfigParser {
    private static final String NODE_CONFIG_PATH_DEFAULT = "node-config.yaml";
    private static final String NODE_CONFIG_ENV_PROPERTY = "mint.config.location";
    private static final Yaml YAML = new Yaml(new Constructor(NodeConfig.class, new LoaderOptions()));

    private ConfigParser() {

    }

    public static NodeConfig parseConfig() {
        String configPath = System.getProperty(NODE_CONFIG_ENV_PROPERTY);

        InputStream inputStream = ConfigParser.class
                .getClassLoader()
                .getResourceAsStream(
                        Objects.requireNonNullElse(configPath, NODE_CONFIG_PATH_DEFAULT)
                );

        NodeConfig nodeConfig = YAML.load(inputStream);

        return nodeConfig;
    }
}
