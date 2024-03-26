plugins {
    `kotlin-dsl`
}

repositories {
    mavenCentral()
    maven("https://plugins.gradle.org/m2")
}

val kotlinVersion = "1.9.21"

dependencies {
    implementation(kotlin("gradle-plugin", kotlinVersion))
    implementation(gradleApi())
}
