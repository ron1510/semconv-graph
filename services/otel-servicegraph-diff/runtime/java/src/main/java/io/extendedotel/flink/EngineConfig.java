package io.extendedotel.flink;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Validated application environment. Credentials never appear in diagnostic output. */
public final class EngineConfig implements Serializable {
  public final String bootstrapServers, inputTopic, outputTopic, groupId;
  public final int ttlSeconds, allowedLatenessSeconds, checkpointIntervalMs, parallelism;
  public final int restartAttempts, restartDelaySeconds;
  public final Map<String, Integer> elementTtls;
  private final Map<String, String> kafkaProperties;

  public static EngineConfig fromEnvironment() {
    return new EngineConfig(System.getenv());
  }

  public EngineConfig(Map<String, String> env) {
    bootstrapServers = text(env, "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
    inputTopic = topic(env, "INTERACTION_DIFF_INPUT_TOPIC", "otel.servicegraph.metrics");
    outputTopic = topic(env, "INTERACTION_DIFF_OUTPUT_TOPIC", "graph.elements.events");
    groupId = topic(env, "INTERACTION_DIFF_GROUP_ID", "graph-element-engine");
    ttlSeconds = integer(env, "INTERACTION_DIFF_TTL_SECONDS", LifecyclePolicy.ONE_DAY_SECONDS, 1);
    allowedLatenessSeconds = integer(env, "INTERACTION_DIFF_ALLOWED_LATENESS_SECONDS", 60, 0);
    checkpointIntervalMs = integer(env, "FLINK_CHECKPOINT_INTERVAL_MS", 30000, 1000);
    parallelism = integer(env, "FLINK_PARALLELISM", 3, 1);
    restartAttempts = integer(env, "FLINK_RESTART_ATTEMPTS", 3, 0);
    restartDelaySeconds = integer(env, "FLINK_RESTART_DELAY_SECONDS", 10, 0);
    Map<String, Integer> ttls = new LinkedHashMap<>();
    CanonicalJson.readObject(env.getOrDefault("GRAPH_ELEMENT_TTL_SECONDS", "{}"))
        .forEach(
            (type, value) -> {
              long ttl = GraphModel.exactLong(value);
              if (!type.matches("[a-z0-9._-]+") || ttl <= 0 || ttl > Integer.MAX_VALUE)
                throw new IllegalArgumentException("invalid graph element TTL");
              ttls.put(type, (int) ttl);
            });
    elementTtls = Map.copyOf(ttls);
    String protocol = text(env, "KAFKA_SECURITY_PROTOCOL", "PLAINTEXT");
    if (!Set.of("PLAINTEXT", "SASL_PLAINTEXT", "SASL_SSL").contains(protocol))
      throw new IllegalArgumentException("unsupported Kafka security protocol");
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put("security.protocol", protocol);
    if (!protocol.equals("PLAINTEXT")) {
      String mechanism = text(env, "KAFKA_SASL_MECHANISM", null);
      if (!mechanism.equals("SCRAM-SHA-256"))
        throw new IllegalArgumentException("unsupported SASL mechanism");
      String username = text(env, "KAFKA_SASL_USERNAME", null);
      String password = env.get("KAFKA_SASL_PASSWORD");
      if (password == null) throw new IllegalArgumentException("missing Kafka SASL password");
      properties.put("sasl.mechanism", mechanism);
      properties.put(
          "sasl.jaas.config",
          "org.apache.kafka.common.security.scram.ScramLoginModule required "
              + "username=\""
              + escapeJaas(username)
              + "\" password=\""
              + escapeJaas(password)
              + "\";");
    } else if (env.containsKey("KAFKA_SASL_MECHANISM")
        || env.containsKey("KAFKA_SASL_USERNAME")
        || env.containsKey("KAFKA_SASL_PASSWORD")) {
      throw new IllegalArgumentException("Kafka authentication requires SASL");
    }
    kafkaProperties = Map.copyOf(properties);
  }

  public Map<String, String> kafkaProperties() {
    return kafkaProperties;
  }

  private static String escapeJaas(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  private static String text(Map<String, String> env, String key, String fallback) {
    String value = env.getOrDefault(key, fallback);
    if (value == null || value.strip().isEmpty())
      throw new IllegalArgumentException("missing setting " + key);
    return value.strip();
  }

  private static String topic(Map<String, String> env, String key, String fallback) {
    String value = text(env, key, fallback);
    if (!value.matches("[A-Za-z0-9._-]+"))
      throw new IllegalArgumentException("invalid topic/group " + key);
    return value;
  }

  private static int integer(Map<String, String> env, String key, int fallback, int minimum) {
    int value = Integer.parseInt(env.getOrDefault(key, Integer.toString(fallback)));
    if (value < minimum) throw new IllegalArgumentException("invalid setting " + key);
    return value;
  }
}
