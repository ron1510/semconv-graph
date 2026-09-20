# syntax=docker/dockerfile:1.7

ARG MAVEN_BASE_IMAGE=maven:3.9.11-eclipse-temurin-17
ARG FLINK_BASE_IMAGE=flink:2.2.1-scala_2.12-java17

FROM ${MAVEN_BASE_IMAGE} AS build
WORKDIR /build
COPY services/otel-servicegraph-diff/runtime/java/pom.xml ./pom.xml
COPY services/otel-servicegraph-diff/runtime/java/src ./src
RUN --mount=type=cache,target=/root/.m2 \
    --mount=type=secret,id=maven_settings,required=false \
    if [ -f /run/secrets/maven_settings ]; then \
      mvn --batch-mode --no-transfer-progress --settings /run/secrets/maven_settings package; \
    else \
      mvn --batch-mode --no-transfer-progress package; \
    fi

FROM ${FLINK_BASE_IMAGE} AS runtime
RUN mkdir -p /opt/flink/usrlib && chmod 0755 /opt/flink/usrlib
COPY --from=build --chown=flink:0 --chmod=0440 \
  /build/target/otel-servicegraph-diff.jar \
  /opt/flink/usrlib/otel-servicegraph-diff.jar
ENV HOME=/tmp
USER 9999:0
