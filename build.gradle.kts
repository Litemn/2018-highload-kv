// See https://gradle.org and https://github.com/gradle/kotlin-dsl

// Apply the java plugin to add support for Java
plugins {
    java
    application
}

repositories {
    jcenter()
}

dependencies {
    // Annotations for better code documentation
    compile("com.intellij:annotations:12.0")

    //one-nio http server
    compile("ru.odnoklassniki:one-nio:1.0.2")

    //rocks db key-value
    compile("org.rocksdb:rocksdbjni:5.14.2")

    //logging
    compile("ch.qos.logback:logback-classic:1.2.3")

    // JUnit Jupiter test framework
    testCompile("org.junit.jupiter:junit-jupiter-api:5.3.1")

    // Guava for tests
    testCompile("com.google.guava:guava:23.1-jre")
}

tasks {
    "test"(Test::class) {
        maxHeapSize = "128m"
        useJUnitPlatform()
    }
}

application {
    // Define the main class for the application
    mainClassName = "ru.mail.polis.Server"

    // And limit Xmx
    applicationDefaultJvmArgs = listOf("-Xmx128m")
}
