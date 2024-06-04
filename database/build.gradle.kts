import com.google.protobuf.gradle.id
import com.google.protobuf.gradle.proto

plugins {
    `java-configuration`
    id("com.google.protobuf") version "0.9.4"
}

repositories {
    mavenCentral()
    mavenLocal()
}

dependencies {
    // Logging
    implementation("org.slf4j:slf4j-api:2.0.12")
    implementation("ch.qos.logback:logback-classic:1.5.3")

    implementation("org.yaml:snakeyaml:2.2")

    implementation("io.grpc:grpc-all:1.63.0")
    compileOnly("org.apache.tomcat:annotations-api:6.0.53")

    implementation("com.google.protobuf:protobuf-java:4.26.0")

    // DI
    implementation("com.google.inject:guice:7.0.0")

    // test
    testImplementation("org.mockito:mockito-core:5.11.0")
    testImplementation("org.powermock:powermock-module-junit4:2.0.9")
    testImplementation("org.powermock:powermock-api-mockito2:2.0.9")
    testImplementation("org.awaitility:awaitility:4.2.1")

}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:4.26.0"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.63.0"
        }
    }
    generateProtoTasks {
        ofSourceSet("main").forEach {
            it.plugins {
                id("grpc") { }
            }
        }
    }
    sourceSets {
        main {
            proto {
                srcDir("src/main/proto")
            }
        }
    }
}

sourceSets {
    main {
        java {
            srcDirs("build/generated/source/proto/main/grpc")
            srcDirs("build/generated/source/proto/main/java")
        }
    }
}

tasks.test {
    testLogging.showStandardStreams = true
}
