# syntax=docker/dockerfile:1.7
FROM maven:3.9.11-eclipse-temurin-17 AS build
WORKDIR /build
COPY services/otel-servicegraph-diff/runtime/java/pom.xml ./pom.xml
COPY services/otel-servicegraph-diff/runtime/java/src ./src
RUN --mount=type=cache,target=/root/.m2 \
    mvn --batch-mode --no-transfer-progress package \
    org.apache.maven.plugins:maven-dependency-plugin:3.8.1:build-classpath \
    -Dmdep.outputFile=target/benchmark-classpath.txt -DincludeScope=test
RUN --mount=type=cache,target=/root/.m2 java -Xms256m -Xmx1g \
    -cp "target/test-classes:target/classes:$(cat target/benchmark-classpath.txt)" \
    io.extendedotel.flink.StateCostBenchmark /build/state-cost.json
FROM scratch
COPY --from=build /build/state-cost.json /state-cost.json
