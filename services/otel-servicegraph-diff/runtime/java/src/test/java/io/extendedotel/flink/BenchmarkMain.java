package io.extendedotel.flink;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Development-only matched-workload measurements; does not measure a Flink cluster. */
public final class BenchmarkMain {
  private BenchmarkMain() {}

  private record Batch(Map<String, GraphModel.State> states, int events) {}

  public static void main(String[] args) throws Exception {
    byte[] payload = Files.readAllBytes(Path.of(args[0]));
    int iterations = Integer.parseInt(args[1]), warmup = Integer.parseInt(args[2]);
    int datapoints = Integer.parseInt(args[3]);
    for (int i = 0; i < warmup; i++) parse(payload);
    var ingestTimes = new ArrayList<Long>();
    List<GraphModel.Contribution> mutations = List.of();
    for (int i = 0; i < iterations; i++) {
      long started = System.nanoTime();
      mutations = parse(payload);
      ingestTimes.add(System.nanoTime() - started);
    }
    Map<String, GraphModel.State> warmStates = Map.of();
    for (int i = 0; i < warmup; i++) warmStates = apply(warmStates, mutations, i).states();
    Map<String, GraphModel.State> states = Map.of();
    var lifecycleTimes = new ArrayList<Long>();
    int events = 0;
    for (int i = 0; i < iterations; i++) {
      long started = System.nanoTime();
      Batch batch = apply(states, mutations, i);
      lifecycleTimes.add(System.nanoTime() - started);
      states = batch.states();
      events += batch.events();
    }
    var statePayloads = new LinkedHashMap<String, Object>();
    states.forEach((id, state) -> statePayloads.put(id, state.toMap()));
    Map<String, Object> report = new LinkedHashMap<>();
    report.put("scope", "in_process_java");
    report.put("java_version", System.getProperty("java.version"));
    report.put(
        "payload_sha256",
        java.util.HexFormat.of()
            .formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(payload)));
    report.put("datapoints", datapoints);
    report.put("iterations", iterations);
    report.put("warmup_iterations", warmup);
    report.put("contributions_per_iteration", mutations.size());
    report.put("events_emitted", events);
    report.put("state_sha256", CanonicalJson.digest(statePayloads));
    report.put("active_elements", states.size());
    report.put(
        "contributor_snapshots",
        states.values().stream().mapToInt(state -> state.contributors().size()).sum());
    report.put("ingest", timing(ingestTimes, datapoints));
    report.put("lifecycle", timing(lifecycleTimes, mutations.size()));
    Files.writeString(Path.of(args[4]), CanonicalJson.stringify(report) + "\n");
  }

  private static List<GraphModel.Contribution> parse(byte[] payload) {
    var parsed = MetricParser.parse(payload);
    if (!parsed.rejections().isEmpty() || parsed.mutations().isEmpty())
      throw new IllegalStateException("benchmark dataset rejected");
    return parsed.mutations();
  }

  private static Batch apply(
      Map<String, GraphModel.State> previous,
      List<GraphModel.Contribution> mutations,
      int iteration) {
    var states = new LinkedHashMap<>(previous);
    int events = 0;
    long processing = 1_800_000_000_000L + iteration;
    for (var mutation : mutations) {
      var result =
          GraphLifecycle.apply(
              states.get(mutation.elementKey()),
              mutation,
              3600,
              mutation.observedAtUnixNano(),
              processing,
              processing);
      if (result.state() == null) throw new IllegalStateException("unexpected deletion");
      states.put(mutation.elementKey(), result.state());
      if (result.event() != null) events++;
    }
    return new Batch(states, events);
  }

  private static Map<String, Object> timing(List<Long> times, int operations) {
    double seconds = times.stream().mapToLong(Long::longValue).sum() / 1e9;
    var sorted = times.stream().sorted().toList();
    return Map.of(
        "total_seconds",
        seconds,
        "operations_per_second",
        operations * times.size() / seconds,
        "batch_latency_ms",
        Map.of(
            "p50",
            sorted.get((int) Math.ceil(sorted.size() * .5) - 1) / 1e6,
            "p95",
            sorted.get((int) Math.ceil(sorted.size() * .95) - 1) / 1e6,
            "p99",
            sorted.get((int) Math.ceil(sorted.size() * .99) - 1) / 1e6));
  }
}
