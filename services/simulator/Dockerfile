# Build context: services/simulator (the image never needs the repository's contracts/).

FROM eclipse-temurin:21-jdk AS build
WORKDIR /build
COPY mvnw pom.xml ./
COPY .mvn .mvn
RUN chmod +x mvnw && ./mvnw -q -B dependency:go-offline
COPY src src
# Tests need Docker (Testcontainers) and ../../contracts, so they run outside the image build.
RUN ./mvnw -q -B -DskipTests package \
    && cp target/simulator-*.jar /build/simulator.jar

# eclipse-temurin:21-jre ships curl, used by the Compose healthcheck on /actuator/health.
FROM eclipse-temurin:21-jre
WORKDIR /app
RUN groupadd --system simulator && useradd --system --gid simulator simulator
COPY --from=build /build/simulator.jar simulator.jar
USER simulator
EXPOSE 8082
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/simulator.jar"]
