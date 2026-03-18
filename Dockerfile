FROM gradle:8.3-jdk17-alpine AS builder

WORKDIR /app
COPY build.gradle.kts settings.gradle.kts /app/
COPY src /app/src
RUN gradle clean build --no-daemon


FROM eclipse-temurin:17-jdk-alpine

WORKDIR /app
COPY --from=builder /app/build/libs/practical-work-kafka-3-*.jar /app/app.jar

CMD ["java", "-jar", "/app/app.jar"]