package io.extendedotel.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Small submission/savepoint commands shared by the Helm hooks; no Python runtime required. */
public final class DeploymentCommands {
  static final String CURRENT_RUNTIME = "java-cbor-v2";
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final Set<String> TERMINAL = Set.of("CANCELED", "FAILED", "FINISHED", "MISSING");
  private static final Pattern SAVEPOINT =
      Pattern.compile("Savepoint completed\\. Path:\\s*(\\S+)");
  private static final HttpClient HTTP =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

  private DeploymentCommands() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 1)
      throw new IllegalArgumentException("Expected inspect, savepoint, or record-runtime");
    Map<String, String> env = System.getenv();
    Path handoff = Path.of(required(env, "FLINK_SAVEPOINT_HANDOFF"));
    Path marker = handoff.getParent().resolve("runtime");
    switch (args[0]) {
      case "inspect" -> inspect(env, marker);
      case "savepoint" -> {
        validateRuntime(marker, true);
        savepoint(env, handoff);
      }
      case "record-runtime" -> writeAtomic(marker, CURRENT_RUNTIME);
      default -> throw new IllegalArgumentException("Unknown deployment command: " + args[0]);
    }
  }

  private static void inspect(Map<String, String> env, Path marker) throws Exception {
    validateRuntime(marker, false);
    String rest = required(env, "FLINK_REST_URL");
    waitFor(rest + "/overview", false);
    String jobId = required(env, "FLINK_JOB_ID");
    String state = jobState(rest, jobId);
    if (!Files.exists(marker)) {
      Path root = marker.getParent().getParent();
      boolean legacy =
          !state.equals("MISSING")
              || hasEntries(root.resolve("checkpoints"))
              || hasEntries(root.resolve("savepoints"))
              || hasSavepointHandoff(marker.getParent());
      if (legacy) incompatible("unmarked/PyFlink");
    }
    if (state.equals("MISSING")) return;
    if (TERMINAL.contains(state)) {
      System.err.printf(
          "Flink job %s is terminal (%s); choose a new fixedJobId for an intentional replacement%n",
          jobId, state);
      System.exit(43);
    }
    System.out.printf(
        "Flink job %s already exists in state %s; skipping submission%n", jobId, state);
    System.exit(42);
  }

  static void validateRuntime(Path marker, boolean upgrade) throws IOException {
    if (Files.isRegularFile(marker)) {
      String previous = Files.readString(marker).strip();
      if (!previous.equals(CURRENT_RUNTIME)) incompatible(previous);
    } else if (upgrade) {
      incompatible("unmarked/PyFlink");
    }
  }

  private static void incompatible(String previous) {
    throw new IllegalStateException(
        "Cannot restore "
            + previous
            + " lifecycle state into the java-cbor-v2 runtime"
            + ". The evidence-only state model requires a clean reset. Follow the rollout in "
            + "docs/operations/upgrades.md; allowNonRestoredState does not migrate serializers.");
  }

  private static boolean hasEntries(Path directory) throws IOException {
    if (!Files.isDirectory(directory)) return false;
    try (var entries = Files.list(directory)) {
      return entries.findAny().isPresent();
    }
  }

  private static boolean hasSavepointHandoff(Path directory) throws IOException {
    if (!Files.isDirectory(directory)) return false;
    try (var entries = Files.list(directory)) {
      return entries.anyMatch(path -> path.getFileName().toString().endsWith(".savepoint"));
    }
  }

  private static void savepoint(Map<String, String> env, Path handoff) throws Exception {
    String rest = required(env, "FLINK_REST_URL");
    String jobId = required(env, "FLINK_JOB_ID");
    Path latest = Path.of(required(env, "FLINK_LATEST_SAVEPOINT_HANDOFF"));
    String state = jobState(rest, jobId);
    if (TERMINAL.contains(state)) {
      Path candidate = nonempty(handoff) ? handoff : latest;
      if (!nonempty(candidate))
        throw new IllegalStateException(
            "Flink job "
                + jobId
                + " is "
                + state
                + " and no preserved upgrade savepoint is available");
      String path = Files.readString(candidate).strip();
      writeAtomic(handoff, path);
      System.out.printf("Flink job %s is %s; reusing preserved savepoint %s%n", jobId, state, path);
      return;
    }
    Process process =
        new ProcessBuilder(
                "/opt/flink/bin/flink",
                "stop",
                "-m",
                required(env, "FLINK_REST_ADDRESS"),
                "--savepointPath",
                "file:///flink-state/savepoints",
                jobId)
            .redirectErrorStream(true)
            .start();
    String output =
        new String(
            process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
    System.out.print(output);
    int exitCode = process.waitFor();
    if (exitCode != 0) System.exit(exitCode);
    Matcher matches = SAVEPOINT.matcher(output);
    String path = null;
    while (matches.find()) path = matches.group(1);
    if (path == null)
      throw new IllegalStateException("Flink stop returned no completed savepoint path");
    writeAtomic(handoff, path);
    writeAtomic(latest, path);
    System.out.printf("Recorded upgrade savepoint %s in %s%n", path, handoff);
  }

  private static boolean nonempty(Path path) throws IOException {
    return Files.isRegularFile(path) && !Files.readString(path).isBlank();
  }

  static void writeAtomic(Path destination, String value) throws IOException {
    Files.createDirectories(destination.getParent());
    Path temporary = destination.resolveSibling(destination.getFileName() + ".tmp");
    Files.writeString(temporary, value + "\n");
    Files.move(
        temporary,
        destination,
        StandardCopyOption.REPLACE_EXISTING,
        StandardCopyOption.ATOMIC_MOVE);
  }

  private static String jobState(String rest, String jobId) throws Exception {
    HttpResponse<String> response = waitFor(rest + "/jobs/" + jobId, true);
    if (response.statusCode() == 404) return "MISSING";
    JsonNode body = JSON.readTree(response.body());
    return body.path("state").asText("UNKNOWN");
  }

  private static HttpResponse<String> waitFor(String url, boolean acceptMissing) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(480).toNanos();
    while (true) {
      try {
        HttpRequest request =
            HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(5)).GET().build();
        HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == 200 || (acceptMissing && response.statusCode() == 404))
          return response;
        throw new IOException("Flink REST returned HTTP " + response.statusCode());
      } catch (IOException exception) {
        if (System.nanoTime() >= deadline)
          throw new IOException("Flink REST did not become ready: " + url, exception);
        Thread.sleep(2000);
      }
    }
  }

  private static String required(Map<String, String> env, String name) {
    String value = env.get(name);
    if (value == null || value.isBlank())
      throw new IllegalArgumentException("Missing environment variable: " + name);
    return value;
  }
}
