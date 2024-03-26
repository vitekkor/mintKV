package com.mint.plugins.java

import com.mint.plugins.PluginAdapter
import com.mint.plugins.apply
import org.gradle.api.JavaVersion
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.tasks.testing.Test
import org.gradle.jvm.toolchain.JavaLanguageVersion
import org.gradle.kotlin.dsl.dependencies
import org.gradle.kotlin.dsl.getByName

class JavaConfigurationPlugin : Plugin<Project> by apply<JavaConfiguration>()

class JavaConfiguration(project: Project) : PluginAdapter(project) {
    override fun Project.apply() {
        pluginManager.apply("java")
        extensions.getByName<JavaPluginExtension>("java").apply {
            sourceCompatibility = JavaVersion.VERSION_21
            targetCompatibility = JavaVersion.VERSION_21
            toolchain {
                languageVersion.set(JavaLanguageVersion.of(21))
            }
        }

        dependencies {
            "testImplementation"(platform("org.junit:junit-bom:5.9.1"))
            "testImplementation"("org.junit.jupiter:junit-jupiter")
        }

        tasks.getByName<Test>("test") {
            allJvmArgs = allJvmArgs.toMutableList().apply { add("--enable-preview") }
            useJUnitPlatform()
        }

        tasks.getByName<JavaCompile>("compileJava") {
            options.compilerArgs.add("--enable-preview")
        }

        tasks.getByName<JavaCompile>("compileTestJava") {
            options.compilerArgs.add("--enable-preview")
        }
    }
}
