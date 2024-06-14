package com.mint.db.raft.integration.configuration

object Configuration {
    val nodes = List(5) { "src/test/resources/node-config-$it.yaml" }
    val nProcesses = nodes.size
}
