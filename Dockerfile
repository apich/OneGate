# syntax=docker/dockerfile:1

FROM maven:3.9.9-eclipse-temurin-17-alpine AS builder
WORKDIR /build

COPY pom.xml .
RUN mvn -B -q dependency:go-offline || true

COPY src ./src
RUN mvn -B -DskipTests clean package

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app

RUN addgroup -S onegate && adduser -S -G onegate onegate
COPY --from=builder /build/target/*.jar /app/app.jar
RUN chown -R onegate:onegate /app

USER onegate
EXPOSE 8080

ENTRYPOINT ["java", "-XX:+UseContainerSupport", "-XX:MaxRAMPercentage=75.0", "-jar", "/app/app.jar"]
