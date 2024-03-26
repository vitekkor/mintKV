import org.gradle.plugin.use.PluginDependenciesSpec

private fun PluginDependenciesSpec.internal(plugin: String) = id("com.mint.plugins.$plugin")

val PluginDependenciesSpec.`java-configuration` get() = internal("java")
