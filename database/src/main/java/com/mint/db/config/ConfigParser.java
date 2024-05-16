package com.mint.db.config;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.representer.Representer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

public final class ConfigParser {
    private static final String NODE_CONFIG_PATH_DEFAULT = "node-config.yaml";
    private static final String NODE_CONFIG_ENV_PROPERTY = "mint.config.location";
    private static final Yaml YAML;

    static {
        Representer representer = new Representer(new DumperOptions());
        representer.getPropertyUtils().setSkipMissingProperties(true);
        LoaderOptions loaderOptions = new LoaderOptions();
        Constructor constructor = new Constructor(NodeConfig.class, loaderOptions);
        YAML = new Yaml(constructor, representer);
    }

    private ConfigParser() {
    }

    public static NodeConfig parseConfig() throws FileNotFoundException {
        String configPath = System.getProperty(NODE_CONFIG_ENV_PROPERTY);

        InputStream inputStream;
        if (configPath != null) {
            inputStream = getConfigInputStream(configPath);
        } else {
            inputStream = ConfigParser.class.getClassLoader().getResourceAsStream(NODE_CONFIG_PATH_DEFAULT);
        }

        NodeConfig nodeConfig = YAML.load(inputStream);

        return nodeConfig;
    }

    public static InputStream getConfigInputStream(String configPath) throws FileNotFoundException {
        File configFile = new File(configPath);
        return new FileInputStream(configFile);
    }

}
