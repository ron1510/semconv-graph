# Run the PyFlink Job Locally

The servicegraph diff job can run directly from PyCharm or a terminal. PyFlink
starts a local Flink MiniCluster, connects it to a local Kafka-compatible
broker, and executes the same job graph used in Kubernetes.

This mode is intended for development and debugging. It does not provide
Kubernetes HA, pod replacement, or persistent checkpoint recovery.

## Prerequisites

Install:

- Python 3.12;
- Java 11;
- Maven 3.9 or later;
- PyCharm, or another Python IDE;
- a locally reachable Kafka or Redpanda broker.

Create these topics before starting the job:

```text
otel.servicegraph.metrics
graph.elements.events
```

Auto topic creation is disabled by the application.

!!! warning

    Use a Python 3.12 interpreter. A virtual environment created with Python
    3.13 or later cannot install this application's pinned PyFlink version.

## Create the Python environment

From the repository root on Windows:

```powershell
py -3.12 -m venv .venv312

.\.venv312\Scripts\python.exe -m pip install `
  -e packages\extended-opentelemetry-semconv `
  -e services\otel-servicegraph-diff
```

On Linux or macOS:

```bash
python3.12 -m venv .venv312

./.venv312/bin/python -m pip install \
  -e packages/extended-opentelemetry-semconv \
  -e services/otel-servicegraph-diff
```

Editable installs make source changes immediately available to the local job.

## Build and install the Java dependencies

The job uses a Java serializer and the Flink Kafka connector. Build both from
the checked-in Maven project:

```powershell
mvn -f services\otel-servicegraph-diff\runtime\java\pom.xml package
```

Locate the Flink library directory inside the virtual environment:

```powershell
$flinkLib = & .\.venv312\Scripts\python.exe -c `
  "from pathlib import Path; import pyflink; print(Path(pyflink.__file__).parent / 'lib')"

Write-Output $flinkLib
```

Copy both runtime JARs into that directory:

```powershell
Copy-Item `
  services\otel-servicegraph-diff\runtime\java\target\interaction-serializer.jar `
  -Destination $flinkLib

Copy-Item `
  services\otel-servicegraph-diff\runtime\java\target\runtime\flink-sql-connector-kafka-5.0.0-2.2.jar `
  -Destination $flinkLib
```

For Linux or macOS, resolve the same directory with
`./.venv312/bin/python` and copy the two JARs with `cp`.

The JARs must be present before the PyFlink Java gateway starts. Restart the
run configuration after rebuilding or replacing either JAR.

## Configure PyCharm

Create a Python run configuration with:

| Setting | Value |
| --- | --- |
| Run kind | Module |
| Module name | `otel_servicegraph_diff.flink_job` |
| Python interpreter | `.venv312` |
| Working directory | Repository root |

Set `JAVA_HOME` to Java 11 and add these environment variables:

```text
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_SECURITY_PROTOCOL=PLAINTEXT
INTERACTION_DIFF_INPUT_TOPIC=otel.servicegraph.metrics
INTERACTION_DIFF_OUTPUT_TOPIC=graph.elements.events
INTERACTION_DIFF_GROUP_ID=interaction-diff-local
INTERACTION_DIFF_TTL_SECONDS=30
INTERACTION_DIFF_ALLOWED_LATENESS_SECONDS=2
FLINK_CHECKPOINT_INTERVAL_MS=5000
FLINK_PARALLELISM=1
FLINK_RESTART_ATTEMPTS=1
FLINK_RESTART_DELAY_SECONDS=1
```

On Windows, also set `PYFLINK_CLIENT_EXECUTABLE` to the absolute path of
`.venv312\Scripts\python.exe`. Ensure the virtual environment's `Scripts`
directory is before other Python installations in `PATH`.

Start the configuration with **Run** or **Debug**. The process remains active
while it consumes Kafka records.

## Run from a terminal

The same module can be run without PyCharm. Set the environment variables from
the previous section and execute:

```powershell
.\.venv312\Scripts\python.exe -m otel_servicegraph_diff.flink_job
```

The local MiniCluster logs are written to the console and the PyFlink log
directory. Locate that directory with:

```powershell
.\.venv312\Scripts\python.exe -c `
  "from pathlib import Path; import pyflink; print(Path(pyflink.__file__).parent / 'log')"
```

## Exercise the job

Send OTLP JSON servicegraph metrics to `otel.servicegraph.metrics`. The easiest
way to generate realistic input is to run the Collector and demo workloads, or
to reuse a captured Collector servicegraph metric.

Consume `graph.elements.events` and verify complete node and edge lifecycle events.

1. New semantic nodes and edges produce complete `upsert` events.
2. Attribute or edge-metric changes produce replacement `upsert` events.
3. Final contributor expiry after `INTERACTION_DIFF_TTL_SECONDS` produces a `delete`.

Use a new `INTERACTION_DIFF_GROUP_ID` when replaying the input topic from its
earliest offset.

## Debugging boundaries

Breakpoints in configuration loading and job-graph construction run in the
PyCharm process. Flink normally runs Python operators in a Python worker
process, so operator breakpoints may require PyCharm subprocess attachment or
a Python remote-debug configuration.

For most graph lifecycle behavior, use the pure transition tests first. Use the
local MiniCluster when validating Kafka serialization, Flink state, timers,
watermarks, or checkpoint behavior.

## Common failures

### Unsupported Python version

Create the virtual environment explicitly with Python 3.12 and select it in
PyCharm. Do not reuse a Python 3.13 or newer environment.

### Java gateway does not start

Confirm `JAVA_HOME` points to a Java 11 JDK and that `java -version` resolves
to it in the PyCharm environment.

### Kafka or serializer class is missing

Confirm both built JARs are in the `pyflink/lib` directory associated with the
selected interpreter, then restart the run configuration.

### Kafka connection succeeds but consumption fails

Check the broker's advertised listener. It must advertise an address reachable
from the host process, such as `localhost:9092`, rather than a Kubernetes
Service name or container-only hostname.

### The production image works but local execution does not

The production image already contains both JARs, Python dependencies, and the
application source. Recheck the local interpreter, JAR directory, Java version,
and broker address.

## Local and Kubernetes differences

| Local PyCharm run | Kubernetes Helm deployment |
| --- | --- |
| Local MiniCluster | Standalone Session cluster |
| One host process and local workers | JobManager and TaskManager Deployments |
| Development Kafka credentials | Secret-backed Kafka configuration |
| No Kubernetes HA | ConfigMap-based Kubernetes HA |
| Local checkpoint lifetime | Checkpoints on the shared RWX claim |
| IDE-oriented debugging | Container stdout and Kubernetes logs |

Use local execution for development. Use the runtime image and Helm chart for
deployment-equivalent and recovery testing.
