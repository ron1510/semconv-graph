package io.extendedotel.flink;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

final class StateSerializerTest {
  @Test
  void cborV2PreservesUnsignedTimeAndNestedAttributesAndCopiesFrames() throws Exception {
    var element =
        GraphModel.Element.edge(
            "edge:a",
            "calls",
            "service:a",
            "service:b",
            Map.of("unicode", "שלום😀", "nested", List.of(Map.of("boolean", true), 0.1)));
    var value =
        new GraphModel.Snapshot(
            new BigInteger("18446744073709551615"),
            new BigInteger("18533144073709551615"),
            86400000L,
            element);
    var serializer = StateSerializer.contributor();
    var binary = new DataOutputSerializer(256);
    serializer.serialize(value, binary);
    assertEquals(2, binary.getCopyOfBuffer()[0]);
    assertEquals(
        value, serializer.deserialize(new DataInputDeserializer(binary.getCopyOfBuffer())));
    var copied = new DataOutputSerializer(256);
    serializer.copy(new DataInputDeserializer(binary.getCopyOfBuffer()), copied);
    assertArrayEquals(binary.getCopyOfBuffer(), copied.getCopyOfBuffer());
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  void snapshotsRejectLegacySerializersOtherKindsAndOldFrameVersions() throws Exception {
    var snapshot = StateSerializer.contributor().snapshotConfiguration();
    assertTrue(
        snapshot
            .resolveSchemaCompatibility(
                (TypeSerializerSnapshot) StringSerializer.INSTANCE.snapshotConfiguration())
            .isIncompatible());
    assertTrue(
        snapshot
            .resolveSchemaCompatibility(
                (TypeSerializerSnapshot) StateSerializer.aggregate().snapshotConfiguration())
            .isIncompatible());
    var bytes = new DataOutputSerializer(64);
    TypeSerializerSnapshot.writeVersionedSnapshot(bytes, snapshot);
    TypeSerializerSnapshot<GraphModel.Snapshot> restored =
        TypeSerializerSnapshot.readVersionedSnapshot(
            new DataInputDeserializer(bytes.getCopyOfBuffer()), getClass().getClassLoader());
    assertTrue(restored.resolveSchemaCompatibility(snapshot).isCompatibleAsIs());
    assertThrows(
        IOException.class,
        () ->
            restored.readSnapshot(
                1, new DataInputDeserializer(new byte[0]), getClass().getClassLoader()));
    assertThrows(
        IOException.class,
        () ->
            StateSerializer.contributor()
                .deserialize(new DataInputDeserializer(new byte[] {1, 0, 0, 0, 0})));
  }
}
