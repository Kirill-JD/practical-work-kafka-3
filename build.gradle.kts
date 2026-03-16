plugins {
    id("java")
}

group = "ru.ycan"
version = "1.0.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    compileOnly("org.projectlombok:lombok:1.18.38")
    annotationProcessor("org.projectlombok:lombok:1.18.38")

    implementation("org.apache.kafka:kafka-clients:3.9.1")
    implementation("org.apache.kafka:kafka-streams:3.9.1")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.20.0")
    implementation("de.undercouch:bson4jackson:2.15.1")
    implementation("org.slf4j:slf4j-api:2.0.17")
    implementation("ch.qos.logback:logback-classic:1.4.14")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}

tasks.jar {
    manifest {
        from("src/main/resources/META-INF/MANIFEST.MF")
    }
    from(configurations.runtimeClasspath.get().map {
        if (it.isDirectory) it else zipTree(it)
    })
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}