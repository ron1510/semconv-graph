package io.extendedotel.flink;

import io.extendedotel.flink.config.EngineConfig;
import io.extendedotel.flink.ingest.MetricParser;
import io.extendedotel.flink.lifecycle.ElementLifecycleFunction;
import io.extendedotel.flink.model.Contribution;
import io.extendedotel.flink.model.Event;
import io.extendedotel.flink.semantic.SemanticRegistry;
import io.extendedotel.flink.serialization.ContributionTypeInformation;
import io.extendedotel.flink.serialization.EventTypeInformation;
import io.extendedotel.flink.transport.GraphEventKafkaSerializer;
import java.time.Duration;
import java.util.Properties;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/** Native session-cluster job. Kafka contracts and keyed lifecycle ownership are unchanged. */
public final class ServiceGraphJob {
  private ServiceGraphJob() {}

  public static void main(String[] args) throws Exception {
    EngineConfig config = EngineConfig.fromEnvironment();
    Configuration flink = new Configuration();
    flink.setString("restart-strategy.type", "fixed-delay");
    flink.setString(
        "restart-strategy.fixed-delay.attempts", Integer.toString(config.restartAttempts));
    flink.setString("restart-strategy.fixed-delay.delay", config.restartDelaySeconds + " s");
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flink);
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
    env.setParallelism(config.parallelism);
    env.enableCheckpointing(config.checkpointIntervalMs);
    configure(env, config);
    env.execute("servicegraph-graph-element-engine");
  }

  static void configure(StreamExecutionEnvironment env, EngineConfig config) {
    for (String type : config.elementTtls.keySet()) {
      if (!SemanticRegistry.INSTANCE.knownElementTypes().contains(type))
        throw new IllegalArgumentException("unknown graph element TTL type: " + type);
    }
    DataStream<Contribution> contributions =
        env.fromSource(
                source(config, config.inputTopic, config.groupId),
                WatermarkStrategy.noWatermarks(),
                "servicegraph-otlp-protobuf")
            .name("servicegraph-kafka-source")
            .uid("graph-kafka-source")
            .flatMap(new MetricParser())
            .returns(ContributionTypeInformation.INSTANCE)
            .name("extract-graph-contributions")
            .uid("graph-extract-contributions");
    contributions
        .assignTimestampsAndWatermarks(
            WatermarkStrategy.<Contribution>forBoundedOutOfOrderness(
                    Duration.ofSeconds(config.allowedLatenessSeconds))
                .withIdleness(Duration.ofSeconds(Math.max(2L * config.allowedLatenessSeconds, 1)))
                .withTimestampAssigner(
                    (value, previous) ->
                        value
                            .observedAtUnixNano()
                            .divide(java.math.BigInteger.valueOf(1_000_000L))
                            .longValueExact()))
        .name("graph-contribution-watermarks")
        .uid("graph-contribution-watermarks")
        .keyBy(Contribution::elementKey)
        .process(new ElementLifecycleFunction(config.ttlSeconds, config.elementTtls))
        .returns(EventTypeInformation.INSTANCE)
        .name("graph-element-lifecycle")
        .uid(ElementLifecycleFunction.UID)
        .sinkTo(sink(config))
        .name("graph-element-events")
        .uid("graph-events-sink");
  }

  static KafkaSource<byte[]> source(EngineConfig config, String topic, String groupId) {
    Properties properties = new Properties();
    properties.putAll(config.kafkaProperties());
    properties.setProperty("commit.offsets.on.checkpoint", "true");
    properties.setProperty("enable.auto.commit", "false");
    properties.setProperty("allow.auto.create.topics", "false");
    return KafkaSource.<byte[]>builder()
        .setBootstrapServers(config.bootstrapServers)
        .setTopics(topic)
        .setGroupId(groupId)
        .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
        .setProperties(properties)
        .setValueOnlyDeserializer(
            new AbstractDeserializationSchema<byte[]>() {
              @Override
              public byte[] deserialize(byte[] message) {
                return message;
              }
            })
        .build();
  }

  static KafkaSink<Event> sink(EngineConfig config) {
    Properties properties = new Properties();
    properties.putAll(config.kafkaProperties());
    properties.setProperty("allow.auto.create.topics", "false");
    return KafkaSink.<Event>builder()
        .setBootstrapServers(config.bootstrapServers)
        .setKafkaProducerConfig(properties)
        .setRecordSerializer(new GraphEventKafkaSerializer(config.outputTopic))
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();
  }
}
