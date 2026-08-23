# syntax=docker/dockerfile:1.7
ARG TINKERPOP_VERSION=3.8.1
ARG ARANGO_TINKERPOP_PROVIDER_VERSION=4.0.0

FROM eclipse-temurin:22-jre-jammy

ARG TINKERPOP_VERSION
ARG ARANGO_TINKERPOP_PROVIDER_VERSION
ARG TINKERPOP_SERVER_URL=https://archive.apache.org/dist/tinkerpop
ARG MAVEN_REPOSITORY_URL=https://repo1.maven.org/maven2

ENV GREMLIN_SERVER_HOME=/opt/gremlin-server \
    GREMLIN_SERVER_PORT=8182

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates curl gettext-base unzip \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "${TINKERPOP_SERVER_URL}/${TINKERPOP_VERSION}/apache-tinkerpop-gremlin-server-${TINKERPOP_VERSION}-bin.zip" \
      -o /tmp/gremlin-server.zip \
    && unzip /tmp/gremlin-server.zip -d /opt \
    && mv "/opt/apache-tinkerpop-gremlin-server-${TINKERPOP_VERSION}" "${GREMLIN_SERVER_HOME}" \
    && rm /tmp/gremlin-server.zip

COPY grapeConfig.xml /root/.groovy/grapeConfig.xml
RUN sed -i "s|@MAVEN_REPOSITORY_URL@|${MAVEN_REPOSITORY_URL}|g" /root/.groovy/grapeConfig.xml \
    && "${GREMLIN_SERVER_HOME}/bin/gremlin-server.sh" install \
      com.arangodb arangodb-tinkerpop-provider "${ARANGO_TINKERPOP_PROVIDER_VERSION}"

RUN groupadd --system --gid 10001 gremlin \
    && useradd --system --uid 10001 --gid gremlin --home-dir "${GREMLIN_SERVER_HOME}" --shell /usr/sbin/nologin gremlin \
    && mkdir -p "${GREMLIN_SERVER_HOME}/runtime-conf" \
    && chown -R gremlin:gremlin "${GREMLIN_SERVER_HOME}"

COPY entrypoint.sh /usr/local/bin/servicegraph-gremlin-entrypoint
RUN chmod 0755 /usr/local/bin/servicegraph-gremlin-entrypoint

WORKDIR ${GREMLIN_SERVER_HOME}
USER 10001:10001
EXPOSE 8182
ENTRYPOINT ["/usr/local/bin/servicegraph-gremlin-entrypoint"]
