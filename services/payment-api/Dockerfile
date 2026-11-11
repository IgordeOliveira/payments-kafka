# syntax=docker/dockerfile:1

# Build context: services/payment-api. Tests are skipped here: they need Docker
# (Testcontainers) and the repository's contracts/ directory, which is outside
# this build context. Run them with ./mvnw verify instead.
FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN chmod +x mvnw
RUN --mount=type=cache,target=/root/.m2 ./mvnw -B -q dependency:go-offline
COPY src/ src/
RUN --mount=type=cache,target=/root/.m2 ./mvnw -B -q -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
RUN groupadd --system app && useradd --system --gid app --no-create-home app
COPY --from=build /workspace/target/payment-api.jar app.jar
USER app
ENV SERVER_PORT=8081
EXPOSE 8081
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=5 \
  CMD curl -fsS "http://localhost:${SERVER_PORT}/actuator/health" || exit 1
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
