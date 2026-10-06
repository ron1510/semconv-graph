package io.extendedotel.flink;

import static org.junit.jupiter.api.Assertions.*;

import io.extendedotel.flink.coalesce.ContributionCoalescer;
import io.extendedotel.flink.config.EngineConfig;
import io.extendedotel.flink.lifecycle.ElementLifecycleFunction;
import io.extendedotel.flink.serialization.ContributionTypeInformation;
import io.extendedotel.flink.serialization.EventTypeInformation;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class ServiceGraphJobTest {
  @Test
  void environmentPreservesTopicsSecurityAndTypedTtlValidation() {
    var config = new EngineConfig(Map.of());
    assertEquals("otel.servicegraph.metrics", config.inputTopic);
    assertEquals("graph.elements.events", config.outputTopic);
    assertEquals("graph-element-engine", config.groupId);
    assertEquals(86400, config.ttlSeconds);
    assertEquals(300, config.coalescingIntervalSeconds);
    assertEquals(Map.of("security.protocol", "PLAINTEXT"), config.kafkaProperties());
    assertThrows(
        IllegalArgumentException.class,
        () -> new EngineConfig(Map.of("KAFKA_SECURITY_PROTOCOL", "SSL")));
    assertThrows(
        IllegalArgumentException.class,
        () -> new EngineConfig(Map.of("KAFKA_SASL_USERNAME", "user")));
    assertThrows(
        IllegalArgumentException.class,
        () -> new EngineConfig(Map.of("GRAPH_ELEMENT_TTL_SECONDS", "{\"service\":1.5}")));
    assertThrows(
        IllegalArgumentException.class,
        () -> new EngineConfig(Map.of("GRAPH_ELEMENT_TTL_SECONDS", "{\"service\":true}")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EngineConfig(
                Map.of(
                    "INTERACTION_DIFF_TTL_SECONDS",
                    "300",
                    "CONTRIBUTION_COALESCING_INTERVAL_SECONDS",
                    "300")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EngineConfig(
                Map.of(
                    "GRAPH_ELEMENT_TTL_SECONDS",
                    "{\"service\":200}",
                    "CONTRIBUTION_COALESCING_INTERVAL_SECONDS",
                    "200")));
    assertEquals(
        0,
        new EngineConfig(Map.of("CONTRIBUTION_COALESCING_INTERVAL_SECONDS", "0"))
            .coalescingIntervalSeconds);
    var sasl =
        new EngineConfig(
            Map.of(
                "KAFKA_SECURITY_PROTOCOL",
                "SASL_SSL",
                "KAFKA_SASL_MECHANISM",
                "SCRAM-SHA-256",
                "KAFKA_SASL_USERNAME",
                "a\"b",
                "KAFKA_SASL_PASSWORD",
                "c\\d"));
    assertEquals(
        "org.apache.kafka.common.security.scram.ScramLoginModule required "
            + "username=\"a\\\"b\" password=\"c\\\\d\";",
        sasl.kafkaProperties().get("sasl.jaas.config"));
  }

  @Test
  void nativeGraphHasExplicitUidsAndOneTypedEvidenceLane() {
    var config =
        new EngineConfig(
            Map.of(
                "GRAPH_ELEMENT_TTL_SECONDS",
                "{\"service\":25}",
                "CONTRIBUTION_COALESCING_INTERVAL_SECONDS",
                "5"));
    var env = StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
    env.setParallelism(1);
    env.enableCheckpointing(config.checkpointIntervalMs);
    ServiceGraphJob.configure(env, config);
    var graph = env.getStreamGraph();
    var uids =
        graph.getStreamNodes().stream()
            .map(node -> node.getTransformationUID())
            .filter(java.util.Objects::nonNull)
            .toList();
    assertTrue(uids.contains(ElementLifecycleFunction.UID));
    assertTrue(uids.contains(ContributionCoalescer.UID));
    assertTrue(uids.contains("graph-kafka-source"));
    assertTrue(uids.contains("graph-events-sink"));
    assertTrue(
        graph.getStreamNodes().stream()
                .map(node -> node.getTypeSerializerOut())
                .filter(ContributionTypeInformation.Serializer.class::isInstance)
                .count()
            >= 2);
    assertTrue(
        graph.getStreamNodes().stream()
            .map(node -> node.getTypeSerializerOut())
            .filter(java.util.Objects::nonNull)
            .anyMatch(EventTypeInformation.Serializer.class::isInstance));
    assertTrue(graph.getCheckpointConfig().isCheckpointingEnabled());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ServiceGraphJob.configure(
                StreamExecutionEnvironment.getExecutionEnvironment(),
                new EngineConfig(Map.of("GRAPH_ELEMENT_TTL_SECONDS", "{\"unknown_type\":5}"))));
  }
}
