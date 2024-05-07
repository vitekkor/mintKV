import com.google.protobuf.gradle.*

plugins {
    id("java")
    id("idea")
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
    implementation ("io.grpc:grpc-protobuf:1.63.0")
    implementation ("io.grpc:grpc-services:1.63.0")
    implementation ("io.grpc:grpc-stub:1.63.0")
//    implementation("io.grpc:grpc-all:1.63.0")
    compileOnly("org.apache.tomcat:annotations-api:6.0.53")

    implementation("com.google.protobuf:protobuf-java:3.6.1")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.6.1"
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
}

sourceSets {
    main {
        java {
            srcDirs("build/generated/source/proto/main/grpc")
            srcDirs("build/generated/source/proto/main/java")
        }
    }
}


