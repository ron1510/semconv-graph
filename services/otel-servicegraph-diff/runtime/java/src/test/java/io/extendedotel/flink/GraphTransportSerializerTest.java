package io.extendedotel.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.extendedotel.flink.GraphModel.Contribution;
import io.extendedotel.flink.GraphModel.Element;
import io.extendedotel.flink.GraphModel.Event;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

final class GraphTransportSerializerTest {
  @Test
  void mutationAndEventRoundTripWithoutRecordReflectionAndRestoreSnapshots() throws Exception {
    Element element =
        Element.edge(
            "edge:雪", "calls", "service:雪", "service:😀", Map.of("array", List.of(1.0, "雪")));
    Contribution mutation =
        new Contribution("contributor", new BigInteger("18446744073709551615"), element);
    assertRoundTrip(new ContributionTypeInformation.Serializer(), mutation);
    Event upsert = GraphLifecycle.apply(null, mutation, 5, 0, 100, 200).event();
    assertRoundTrip(new EventTypeInformation.Serializer(), upsert);
    var active = GraphLifecycle.apply(null, mutation, 5, 0, 100, 200).state();
    Event deletion =
        GraphLifecycle.expire(
                active,
                GraphLifecycle.ExpiryClock.EVENT_TIME,
                active.contributors().get("contributor").eventExpiresAtUnixNano(),
                201)
            .event();
    assertRoundTrip(new EventTypeInformation.Serializer(), deletion);
  }

  private static <T> void assertRoundTrip(TypeSerializer<T> serializer, T original)
      throws Exception {
    DataOutputSerializer output = new DataOutputSerializer(128);
    serializer.serialize(original, output);
    assertEquals(
        original, serializer.deserialize(new DataInputDeserializer(output.getCopyOfBuffer())));
    assertEquals(original, serializer.copy(original));
    DataOutputSerializer copied = new DataOutputSerializer(128);
    serializer.copy(new DataInputDeserializer(output.getCopyOfBuffer()), copied);
    assertEquals(
        original, serializer.deserialize(new DataInputDeserializer(copied.getCopyOfBuffer())));

    DataOutputSerializer snapshotOutput = new DataOutputSerializer(128);
    TypeSerializerSnapshot.writeVersionedSnapshot(
        snapshotOutput, serializer.snapshotConfiguration());
    TypeSerializerSnapshot<T> restored =
        TypeSerializerSnapshot.readVersionedSnapshot(
            new DataInputDeserializer(snapshotOutput.getCopyOfBuffer()),
            GraphTransportSerializerTest.class.getClassLoader());
    assertTrue(
        restored.resolveSchemaCompatibility(serializer.snapshotConfiguration()).isCompatibleAsIs());
    assertEquals(
        original,
        restored
            .restoreSerializer()
            .deserialize(new DataInputDeserializer(output.getCopyOfBuffer())));
  }
}
