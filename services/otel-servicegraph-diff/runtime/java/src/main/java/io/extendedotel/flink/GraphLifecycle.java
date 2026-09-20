package io.extendedotel.flink;

import io.extendedotel.flink.GraphModel.Element;
import io.extendedotel.flink.GraphModel.Event;
import io.extendedotel.flink.GraphModel.Result;
import io.extendedotel.flink.GraphModel.Snapshot;
import io.extendedotel.flink.GraphModel.State;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Contributor merge and lifecycle decisions, independent of Flink state and clocks. */
public final class GraphLifecycle {
  public enum ExpiryClock {
    EVENT_TIME,
    PROCESSING_TIME
  }

  private GraphLifecycle() {}

  public static Result apply(
      State previous,
      GraphModel.Contribution contribution,
      int defaultTtlSeconds,
      long eventExpiryBaseUnixNano,
      long processingTimeUnixMs,
      long emittedAtUnixMs) {
    return apply(
        previous,
        contribution,
        defaultTtlSeconds,
        BigInteger.valueOf(eventExpiryBaseUnixNano),
        processingTimeUnixMs,
        emittedAtUnixMs);
  }

  public static Result apply(
      State previous,
      GraphModel.Contribution contribution,
      int defaultTtlSeconds,
      BigInteger eventExpiryBaseUnixNano,
      long processingTimeUnixMs,
      long emittedAtUnixMs) {
    if (defaultTtlSeconds <= 0) {
      throw new IllegalArgumentException("default contributor TTL must be greater than zero");
    }
    Snapshot incoming =
        LifecyclePolicy.snapshot(
            contribution, defaultTtlSeconds, eventExpiryBaseUnixNano, processingTimeUnixMs);
    Snapshot existing =
        previous == null ? null : previous.contributors().get(contribution.contributorId());
    if (existing != null
        && contribution.observedAtUnixNano().compareTo(existing.observedAtUnixNano()) < 0) {
      return new Result(previous, null);
    }
    if (previous != null) {
      validateIdentity(previous, contribution.element());
    }
    Map<String, Snapshot> contributors =
        previous == null ? new LinkedHashMap<>() : new LinkedHashMap<>(previous.contributors());
    contributors.put(contribution.contributorId(), incoming);
    return updated(
        previous,
        contribution.element().id(),
        contributors,
        contribution.observedAtUnixNano(),
        emittedAtUnixMs);
  }

  public static Result expire(
      State previous, ExpiryClock clock, long timestamp, long emittedAtUnixMs) {
    return expire(previous, clock, BigInteger.valueOf(timestamp), emittedAtUnixMs);
  }

  public static Result expire(
      State previous, ExpiryClock clock, BigInteger timestamp, long emittedAtUnixMs) {
    Map<String, Snapshot> contributors = new LinkedHashMap<>();
    BigInteger observedAt = BigInteger.ZERO;
    boolean expired = false;
    for (Map.Entry<String, Snapshot> entry : previous.contributors().entrySet()) {
      Snapshot snapshot = entry.getValue();
      BigInteger deadline =
          clock == ExpiryClock.EVENT_TIME
              ? snapshot.eventExpiresAtUnixNano()
              : snapshot.processingExpiresAtUnixMs() == null
                  ? null
                  : BigInteger.valueOf(snapshot.processingExpiresAtUnixMs());
      if (deadline != null && deadline.compareTo(timestamp) <= 0) {
        expired = true;
        observedAt =
            observedAt.max(
                snapshot.eventExpiresAtUnixNano() == null
                    ? snapshot.observedAtUnixNano()
                    : snapshot.eventExpiresAtUnixNano());
      } else {
        contributors.put(entry.getKey(), snapshot);
      }
    }
    if (!expired) {
      return new Result(previous, null);
    }
    if (contributors.isEmpty()) {
      return new Result(null, deleteEvent(previous.elementId(), observedAt, emittedAtUnixMs));
    }
    return updated(previous, previous.elementId(), contributors, observedAt, emittedAtUnixMs);
  }

  public static String payloadHash(Element element) {
    return CanonicalJson.digest(element.toMap());
  }

  private static Result updated(
      State previous,
      String elementId,
      Map<String, Snapshot> contributors,
      BigInteger observedAt,
      long emittedAt) {
    Element element = merge(elementId, contributors);
    String hash = payloadHash(element);
    State state = new State(elementId, contributors, hash);
    if (previous != null && previous.lastPayloadHash().equals(hash)) {
      return new Result(state, null);
    }
    return new Result(
        state,
        new GraphModel.Upsert(
            eventId("upsert", elementId, observedAt, hash), observedAt, emittedAt, hash, element));
  }

  static Element merge(String elementId, Map<String, Snapshot> contributors) {
    Map<String, Map.Entry<String, Snapshot>> winners = new LinkedHashMap<>();
    Element identity = contributors.values().iterator().next().element();
    for (Map.Entry<String, Snapshot> contributor : contributors.entrySet()) {
      for (String attribute : contributor.getValue().element().attributes().keySet()) {
        Map.Entry<String, Snapshot> previous = winners.get(attribute);
        if (previous == null
            || wins(
                contributor.getKey(),
                contributor.getValue(),
                previous.getKey(),
                previous.getValue())) {
          winners.put(attribute, contributor);
        }
      }
    }
    Map<String, Object> attributes = new LinkedHashMap<>();
    winners.forEach(
        (name, winner) -> attributes.put(name, winner.getValue().element().attributes().get(name)));
    return identity instanceof GraphModel.Node
        ? Element.node(elementId, identity.type(), attributes)
        : Element.edge(
            elementId, identity.type(), identity.sourceId(), identity.targetId(), attributes);
  }

  static boolean wins(String candidateId, Snapshot candidate, String currentId, Snapshot current) {
    int timestamp = candidate.observedAtUnixNano().compareTo(current.observedAtUnixNano());
    return timestamp > 0
        || timestamp == 0 && CanonicalJson.compareStrings(candidateId, currentId) < 0;
  }

  private static void validateIdentity(State previous, Element current) {
    Element existing = previous.contributors().values().iterator().next().element();
    if (!previous.elementId().equals(current.id())
        || !existing.kind().equals(current.kind())
        || !existing.type().equals(current.type())
        || !Objects.equals(existing.sourceId(), current.sourceId())
        || !Objects.equals(existing.targetId(), current.targetId())) {
      throw new IllegalArgumentException(
          "contribution conflicts with graph element identity " + previous.elementId());
    }
  }

  private static Event deleteEvent(String elementId, BigInteger observedAt, long emittedAt) {
    return new GraphModel.Delete(
        eventId("delete", elementId, observedAt, null), elementId, observedAt, emittedAt);
  }

  static String eventId(String operation, String elementId, BigInteger observedAt, String hash) {
    Map<String, Object> value = new LinkedHashMap<>();
    value.put("operation", operation);
    value.put("element_id", elementId);
    value.put("observed_at_unix_nano", observedAt);
    value.put("payload_hash", hash);
    return CanonicalJson.digest(value);
  }
}
