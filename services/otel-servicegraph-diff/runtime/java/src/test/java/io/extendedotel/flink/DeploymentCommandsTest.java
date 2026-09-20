package io.extendedotel.flink;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DeploymentCommandsTest {
  @TempDir Path directory;

  @Test
  void legacyPythonCannotBeUpgradedToJavaWithoutReset() {
    var exception =
        assertThrows(
            IllegalStateException.class,
            () -> DeploymentCommands.validateRuntime(directory.resolve("runtime"), true));
    assertTrue(exception.getMessage().contains("clean reset"));
  }

  @Test
  void newInstallAndCompatibleJavaUpgradeAreAllowed() throws Exception {
    Path marker = directory.resolve("upgrades/runtime");
    assertDoesNotThrow(() -> DeploymentCommands.validateRuntime(marker, false));
    DeploymentCommands.writeAtomic(marker, DeploymentCommands.CURRENT_RUNTIME);
    assertDoesNotThrow(() -> DeploymentCommands.validateRuntime(marker, true));
  }

  @Test
  void legacyAndUnknownMarkersAreRejected() throws Exception {
    Path marker = directory.resolve("runtime");
    DeploymentCommands.writeAtomic(marker, "python");
    assertThrows(
        IllegalStateException.class, () -> DeploymentCommands.validateRuntime(marker, false));
    DeploymentCommands.writeAtomic(marker, "unknown");
    assertThrows(
        IllegalStateException.class, () -> DeploymentCommands.validateRuntime(marker, true));
  }

  @Test
  void freshInstallMustNotRecordRuntimeUntilSubmissionSucceeds() throws Exception {
    Result result = command("inspect", "MISSING");
    assertEquals(0, result.exitCode(), result.output());
    assertFalse(Files.exists(directory.resolve("upgrades/runtime")));
    assertEquals(0, command("record-runtime", "MISSING").exitCode());
    assertEquals(
        DeploymentCommands.CURRENT_RUNTIME + "\n",
        Files.readString(directory.resolve("upgrades/runtime")));
  }

  @Test
  void unmarkedRunningJobIsRejectedRatherThanSilentlySkipped() throws Exception {
    Result result = command("inspect", "RUNNING");
    assertTrue(result.exitCode() != 0);
    assertTrue(result.output().contains("clean reset"), result.output());
  }

  @Test
  void legacyCheckpointFilesRejectFreshJavaSubmission() throws Exception {
    Files.createDirectories(directory.resolve("checkpoints/old-job"));
    Result result = command("inspect", "MISSING");
    assertTrue(result.exitCode() != 0);
    assertTrue(result.output().contains("clean reset"), result.output());
  }

  @Test
  void compatibleRunningJobSkipsAndTerminalJobRejectsDuplicateId() throws Exception {
    DeploymentCommands.writeAtomic(
        directory.resolve("upgrades/runtime"), DeploymentCommands.CURRENT_RUNTIME);
    assertEquals(42, command("inspect", "RUNNING").exitCode());
    assertEquals(43, command("inspect", "FAILED").exitCode());
  }

  @Test
  void failedUpgradeReusesPreservedSavepointWithoutCallingStopAgain() throws Exception {
    DeploymentCommands.writeAtomic(
        directory.resolve("upgrades/runtime"), DeploymentCommands.CURRENT_RUNTIME);
    DeploymentCommands.writeAtomic(
        directory.resolve("upgrades/latest.savepoint"), "file:///flink-state/savepoints/java-v1");
    Result result = command("savepoint", "MISSING");
    assertEquals(0, result.exitCode(), result.output());
    assertEquals(
        "file:///flink-state/savepoints/java-v1\n",
        Files.readString(directory.resolve("upgrades/revision-2.savepoint")));
  }

  private record Result(int exitCode, String output) {}

  private Result command(String command, String state) throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          boolean missing =
              exchange.getRequestURI().getPath().startsWith("/jobs/") && state.equals("MISSING");
          byte[] body = ("{\"state\":\"" + state + "\"}").getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(missing ? 404 : 200, body.length);
          try (var output = exchange.getResponseBody()) {
            output.write(body);
          }
        });
    server.start();
    Process process = null;
    try {
      String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
      ProcessBuilder builder =
          new ProcessBuilder(
                  java,
                  "-cp",
                  System.getProperty("java.class.path"),
                  DeploymentCommands.class.getName(),
                  command)
              .redirectErrorStream(true);
      var env = builder.environment();
      env.put("FLINK_REST_URL", "http://127.0.0.1:" + server.getAddress().getPort());
      env.put("FLINK_JOB_ID", "00000000000000000000000000000001");
      env.put(
          "FLINK_SAVEPOINT_HANDOFF", directory.resolve("upgrades/revision-2.savepoint").toString());
      env.put(
          "FLINK_LATEST_SAVEPOINT_HANDOFF",
          directory.resolve("upgrades/latest.savepoint").toString());
      process = builder.start();
      assertTrue(process.waitFor(15, TimeUnit.SECONDS), "deployment helper did not terminate");
      return new Result(
          process.exitValue(),
          new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
    } finally {
      if (process != null && process.isAlive()) process.destroyForcibly();
      server.stop(0);
    }
  }
}
