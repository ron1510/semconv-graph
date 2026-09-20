package io.extendedotel.flink;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Map;

/** Contributor lifetime rules; no Flink state or wall-clock ownership. */
public final class LifecyclePolicy implements Serializable {
  public static final int ONE_DAY_SECONDS = 86_400;
  private static final BigInteger NANOS_PER_SECOND = BigInteger.valueOf(1_000_000_000L);

  private final int defaultSeconds;
  private final Map<String, Integer> typeSeconds;

  public LifecyclePolicy(int defaultSeconds, Map<String, Integer> typeSeconds) {
    if (defaultSeconds <= 0 || typeSeconds.values().stream().anyMatch(value -> value <= 0)) {
      throw new IllegalArgumentException("lifecycle TTL must be greater than zero");
    }
    this.defaultSeconds = defaultSeconds;
    this.typeSeconds = Map.copyOf(typeSeconds);
  }

  public int ttlSeconds(GraphModel.Element element) {
    if (element instanceof GraphModel.Node) {
      return typeSeconds.getOrDefault(element.type(), defaultSeconds);
    }
    int endpoints =
        Math.min(
            typeSeconds.getOrDefault(entityType(element.sourceId()), defaultSeconds),
            typeSeconds.getOrDefault(entityType(element.targetId()), defaultSeconds));
    return Math.min(typeSeconds.getOrDefault(element.type(), endpoints), endpoints);
  }

  public GraphModel.Snapshot snapshot(
      GraphModel.Contribution contribution, BigInteger watermarkUnixNano, long processingUnixMs) {
    return snapshot(
        contribution, ttlSeconds(contribution.element()), watermarkUnixNano, processingUnixMs);
  }

  static GraphModel.Snapshot snapshot(
      GraphModel.Contribution observation,
      int seconds,
      BigInteger watermarkUnixNano,
      long processingUnixMs) {
    BigInteger eventDeadline =
        observation
            .observedAtUnixNano()
            .max(watermarkUnixNano)
            .add(BigInteger.valueOf(seconds).multiply(NANOS_PER_SECOND));
    long processingDeadline =
        Math.addExact(processingUnixMs, Math.multiplyExact((long) seconds, 1_000L));
    return new GraphModel.Snapshot(
        observation.observedAtUnixNano(), eventDeadline, processingDeadline, observation.element());
  }

  private static String entityType(String id) {
    int separator = id.indexOf(':');
    if (separator < 1) {
      throw new IllegalArgumentException("graph node ID has no semantic type prefix");
    }
    return id.substring(0, separator);
  }
}
