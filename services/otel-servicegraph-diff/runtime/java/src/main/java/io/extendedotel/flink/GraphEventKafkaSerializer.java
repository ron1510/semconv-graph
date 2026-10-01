package io.extendedotel.flink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

/** Element-keyed schema 3.0 events; deletes are explicit events, not Kafka tombstones. */
public final class GraphEventKafkaSerializer
    implements KafkaRecordSerializationSchema<GraphModel.Event> {
  private static final ObjectMapper JSON = new ObjectMapper();
  private final String topic;

  public GraphEventKafkaSerializer(String topic) {
    this.topic = topic;
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(
      GraphModel.Event event, KafkaSinkContext context, Long timestamp) {
    try {
      return new ProducerRecord<>(
          topic,
          null,
          timestamp,
          event.elementId().getBytes(java.nio.charset.StandardCharsets.UTF_8),
          JSON.writeValueAsBytes(event.toMap()));
    } catch (JsonProcessingException exception) {
      throw new IllegalArgumentException("graph event cannot be encoded as JSON", exception);
    }
  }
}
