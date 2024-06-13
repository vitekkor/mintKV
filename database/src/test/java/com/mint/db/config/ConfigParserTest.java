package com.mint.db.config;

import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConfigParserTest {
    private static final String NODE_CONFIG_ENV_PROPERTY = "mint.config.location";

    @Test
    void testParseConfig_DefaultPath() throws FileNotFoundException {
        NodeConfig expectedConfig = new NodeConfig(9090, 8080, 1, "db/", 200L, Arrays.asList("node1", "node2"));

        NodeConfig actualConfig = ConfigParser.parseConfig();

        assertEquals(expectedConfig.getPort(), actualConfig.getPort());
        assertEquals(expectedConfig.getNodeId(), actualConfig.getNodeId());
        assertEquals(expectedConfig.getLogDir(), actualConfig.getLogDir());
        assertEquals(expectedConfig.getCluster(), actualConfig.getCluster());
    }

    @Test
    void testParseConfig_CustomPath() throws FileNotFoundException {
        String customPath = "src/test/resources/custom-config.yaml";
        System.setProperty(NODE_CONFIG_ENV_PROPERTY, customPath);

        try {
            NodeConfig expectedConfig = new NodeConfig(
                    9090,
                    8800,
                    11,
                    "/var/logs2",
                    400L,
                    Arrays.asList(
                            "node1",
                            "node2",
                            "node3",
                            "node4",
                            "node5",
                            "node6",
                            "node7",
                            "node8",
                            "node9",
                            "node10",
                            "node11"
                    )
            );

            NodeConfig actualConfig = ConfigParser.parseConfig();

            assertEquals(expectedConfig.getPort(), actualConfig.getPort());
            assertEquals(expectedConfig.getNodeId(), actualConfig.getNodeId());
            assertEquals(expectedConfig.getLogDir(), actualConfig.getLogDir());
            assertEquals(expectedConfig.getCluster(), actualConfig.getCluster());
        } finally {
            System.clearProperty(NODE_CONFIG_ENV_PROPERTY);
        }
    }

    @Test
    void testGetConfigInputStream_FileNotFound() {
        String invalidPath = "invalid-path.yaml";

        assertThrows(FileNotFoundException.class, () -> ConfigParser.getConfigInputStream(invalidPath));
    }
}
