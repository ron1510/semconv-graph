# Run Flink Locally

The production application is a native Java Flink 2.2.1 job built for Java 17.
Use Maven tests for pure behavior and operator harness checks, and a local
Flink 2.2.1 Session cluster for Kafka/state debugging:

```console
mvn -f services/otel-servicegraph-diff/runtime/java/pom.xml verify
flink run --class io.extendedotel.flink.ServiceGraphJob \
  services/otel-servicegraph-diff/runtime/java/target/otel-servicegraph-diff.jar
```

Set the Kafka, lifecycle, and checkpoint variables described in
[Flink configuration](../configuration/flink.md) in the submitter and worker
environment. The executable JAR includes application
dependencies; the Flink distribution supplies Flink runtime classes. A local
Session cluster does not provide Kubernetes HA or shared-claim recovery.
Use the Helm-based lifecycle tests for deployment and recovery proof.

## Local pipeline and debugging

Run `python -m tools.local_demo up` to build the Java image and provision a
disposable Kind pipeline. Use `python -m tools.local_demo down` to remove that
environment. The SDK, indexer and local provisioning tools still use Python;
the Flink application has no Python dependency.

Open `services/otel-servicegraph-diff/runtime/java` as a Maven project in a Java
IDE and use JDK 17. `ServiceGraphJob` wires Kafka, parsing, semantic extraction,
keyed state/timers and output. `SemanticRegistry` reads generated entity
metadata; `GraphModel.Element` represents nodes and edges. Run the operator
tests on Linux, including through the Docker image build, for RocksDB JNI.

For general pipeline assertions, run:

```console
python -m pytest tests/e2e -m e2e --run-e2e
```

The old Python Flink source and its setup are available at Git revision
`15124a6`. Its checkpoints cannot be restored by the Java job.
