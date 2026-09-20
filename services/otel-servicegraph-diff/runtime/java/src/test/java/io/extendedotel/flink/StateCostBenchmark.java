package io.extendedotel.flink;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

/** Developer measurement of state codec and retained-key work, not cluster capacity. */
public final class StateCostBenchmark {
  private StateCostBenchmark() {}

  public static void main(String[] args) throws Exception {
    Map<String, Object> attributes = new LinkedHashMap<>();
    attributes.put("service.name", "retained-service");
    attributes.put("service.version", "1.0");
    for (int index = 0; index < 16; index++) {
      attributes.put("description." + index, "dimension-" + index + "-雪-" + "x".repeat(48));
    }
    GraphModel.Element node =
        GraphModel.Element.node("service:retained-service", "service", attributes);
    BigInteger observed = new BigInteger("1789657544000000000");
    GraphModel.Snapshot sample =
        new GraphModel.Snapshot(
            observed,
            observed.add(BigInteger.valueOf(86_400_000_000_000L)),
            1_789_743_944_000L,
            node);
    String json = CanonicalJson.stringify(sample.toMap());
    long checksum = 0;
    for (int index = 0; index < 1000; index++) {
      checksum +=
          GraphModel.Snapshot.fromMap(CanonicalJson.readObject(json)).element().attributes().size();
    }
    long started = System.nanoTime();
    for (int index = 0; index < 5000; index++) {
      String encoded = CanonicalJson.stringify(sample.toMap());
      checksum +=
          GraphModel.Snapshot.fromMap(CanonicalJson.readObject(encoded))
              .element()
              .attributes()
              .size();
    }
    long codecNanos = System.nanoTime() - started;
    var binarySerializer = StateSerializer.contributor();
    var bytes = new DataOutputSerializer(2048);
    binarySerializer.serialize(sample, bytes);
    int binaryBytes = bytes.length();
    for (int index = 0; index < 1000; index++) {
      bytes.clear();
      binarySerializer.serialize(sample, bytes);
      checksum +=
          binarySerializer
              .deserialize(new DataInputDeserializer(bytes.getCopyOfBuffer()))
              .element()
              .attributes()
              .size();
    }
    long binaryStarted = System.nanoTime();
    for (int index = 0; index < 5000; index++) {
      bytes.clear();
      binarySerializer.serialize(sample, bytes);
      checksum +=
          binarySerializer
              .deserialize(new DataInputDeserializer(bytes.getCopyOfBuffer()))
              .element()
              .attributes()
              .size();
    }
    long binaryNanos = System.nanoTime() - binaryStarted;
    List<Map<String, Object>> retained = new ArrayList<>();
    for (int cardinality : new int[] {256, 4096, 16384}) {
      Map<String, GraphModel.Snapshot> snapshots = new LinkedHashMap<>();
      for (int index = 0; index < cardinality; index++) {
        BigInteger timestamp = observed.add(BigInteger.valueOf(index));
        snapshots.put(
            "contributor-" + index,
            new GraphModel.Snapshot(
                timestamp,
                timestamp.add(BigInteger.valueOf(86_400_000_000_000L)),
                sample.processingExpiresAtUnixMs(),
                node));
      }
      GraphModel.State state =
          new GraphModel.State(node.id(), snapshots, GraphLifecycle.payloadHash(node));
      GraphModel.Contribution refresh =
          new GraphModel.Contribution(
              "contributor-0", observed.add(BigInteger.valueOf(cardinality)), node);
      for (int index = 0; index < 10; index++) {
        state =
            GraphLifecycle.apply(state, refresh, 86_400, observed, 1_789_657_544_000L, 0).state();
      }
      long mergeStarted = System.nanoTime();
      for (int index = 0; index < 50; index++) {
        state =
            GraphLifecycle.apply(state, refresh, 86_400, observed, 1_789_657_544_000L, 0).state();
        checksum += state.contributors().size();
      }
      double fullMergeUs = (System.nanoTime() - mergeStarted) / 50_000.0;
      AttributeWinners winners = AttributeWinners.rebuild(state);
      Map<String, GraphModel.Snapshot> pointState = new LinkedHashMap<>(state.contributors());
      long[] reads = {0};
      long indexedStarted = System.nanoTime();
      for (int index = 0; index < 1100; index++) {
        if (index == 100) {
          indexedStarted = System.nanoTime();
          reads[0] = 0;
        }
        String contributor = "contributor-" + (index % 2);
        var next =
            new GraphModel.Snapshot(
                observed.add(BigInteger.valueOf(cardinality + index + 1)),
                sample.eventExpiresAtUnixNano(),
                sample.processingExpiresAtUnixMs(),
                node);
        Map<String, GraphModel.Snapshot> perObservation = new LinkedHashMap<>();
        winners =
            winners.observe(
                contributor,
                next,
                true,
                id ->
                    perObservation.computeIfAbsent(
                        id,
                        key -> {
                          reads[0]++;
                          return pointState.get(key);
                        }));
        pointState.put(contributor, next);
        checksum += winners.contributorCount();
      }
      double indexedUs = (System.nanoTime() - indexedStarted) / 1_000_000.0;
      Map<String, Object> entry = new LinkedHashMap<>();
      entry.put("contributors", cardinality);
      entry.put("full_merge_refresh_mean_us", fullMergeUs);
      entry.put("indexed_refresh_mean_us", indexedUs);
      entry.put("indexed_winner_point_reads_per_refresh", reads[0] / 1000.0);
      entry.put(
          "estimated_json_value_bytes",
          (long) json.getBytes(java.nio.charset.StandardCharsets.UTF_8).length * cardinality);
      entry.put("estimated_binary_value_bytes", (long) binaryBytes * cardinality);
      entry.put("payload_hash", state.lastPayloadHash());
      if (!GraphLifecycle.payloadHash(winners.element()).equals(state.lastPayloadHash())) {
        throw new AssertionError("indexed benchmark changed aggregate payload");
      }
      retained.add(entry);
    }
    Map<String, Object> report = new LinkedHashMap<>();
    report.put("scope", "single_jvm_state_codec_and_pure_merge");
    report.put("java_version", System.getProperty("java.version"));
    report.put("retention_seconds", 86_400);
    report.put(
        "json_snapshot_bytes", json.getBytes(java.nio.charset.StandardCharsets.UTF_8).length);
    report.put("json_roundtrip_mean_us", codecNanos / 5_000_000.0);
    report.put("binary_snapshot_bytes", binaryBytes);
    report.put("binary_roundtrip_mean_us", binaryNanos / 5_000_000.0);
    report.put("retained_keys", retained);
    report.put("checksum", checksum);
    report.put(
        "limitations",
        List.of(
            "Warm JVM, fixed 1GiB heap; synthetic descriptions, not a capacity measurement.",
            "Pure/indexed merges exclude RocksDB reads, state writes, timer scans and checkpoints.",
            "Twenty-four-hour deadlines are logical clocks; this is not a day-long physical"
                + " soak."));
    Files.writeString(Path.of(args[0]), CanonicalJson.stringify(report) + "\n");
  }
}
