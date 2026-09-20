package io.extendedotel.flink;

import java.nio.charset.StandardCharsets;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

/** Element-keyed schema 3.0 events; deletes are explicit events, not Kafka tombstones. */
public final class GraphEventKafkaSerializer
    implements KafkaRecordSerializationSchema<GraphModel.Event> {
  private final String topic;

  public GraphEventKafkaSerializer(String topic) {
    this.topic = topic;
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(
      GraphModel.Event event, KafkaSinkContext context, Long timestamp) {
    return new ProducerRecord<>(
        topic,
        null,
        timestamp,
        event.elementId().getBytes(StandardCharsets.UTF_8),
        event.toJson().getBytes(StandardCharsets.UTF_8));
  }
}
