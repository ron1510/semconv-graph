package io.extendedotel.flink.serialization;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import io.extendedotel.flink.lifecycle.AttributeWinners;
import io.extendedotel.flink.model.Aggregate;
import io.extendedotel.flink.model.Contribution;
import io.extendedotel.flink.model.Element;
import io.extendedotel.flink.model.Event;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Map;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/** Typed, length-framed CBOR state for the clean Java runtime. */
public class StateSerializer<T> extends TypeSerializer<T> {
  enum Kind {
    CONTRIBUTOR,
    AGGREGATE,
    WINNERS,
    CONTRIBUTION,
    EVENT
  }

  private static final int FORMAT_VERSION = 4;
  private static final int MAX_FRAME_BYTES = 64 * 1024 * 1024;
  private static final ObjectMapper MAPPER =
      new ObjectMapper(new CBORFactory()).enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  private static final TypeReference<Map<String, Object>> OBJECT = new TypeReference<>() {};
  private final Kind kind;

  protected StateSerializer(Kind kind) {
    this.kind = kind;
  }

  public static StateSerializer<io.extendedotel.flink.model.Snapshot> contributor() {
    return new StateSerializer<>(Kind.CONTRIBUTOR);
  }

  public static StateSerializer<Aggregate> aggregate() {
    return new StateSerializer<>(Kind.AGGREGATE);
  }

  public static StateSerializer<AttributeWinners> winners() {
    return new StateSerializer<>(Kind.WINNERS);
  }

  private Map<String, Object> fields(T value) {
    return switch (kind) {
      case CONTRIBUTOR -> ((io.extendedotel.flink.model.Snapshot) value).toMap();
      case AGGREGATE -> ((Aggregate) value).toMap();
      case WINNERS -> winnerFields((AttributeWinners) value);
      case CONTRIBUTION -> ((Contribution) value).toMap();
      case EVENT -> ((Event) value).toMap();
    };
  }

  @SuppressWarnings(
      "unchecked") // Kind is fixed by typed factories and persisted in the serializer snapshot.
  private T record(Map<String, Object> fields) {
    return (T)
        switch (kind) {
          case CONTRIBUTOR -> io.extendedotel.flink.model.Snapshot.fromMap(fields);
          case AGGREGATE -> Aggregate.fromMap(fields);
          case WINNERS -> winnerRecord(fields);
          case CONTRIBUTION -> Contribution.fromMap(fields);
          case EVENT -> Event.fromMap(fields);
        };
  }

  private static Map<String, Object> winnerFields(AttributeWinners value) {
    return Map.of(
        "element",
        value.element().toMap(),
        "owners",
        value.owners(),
        "contributor_count",
        value.contributorCount());
  }

  private static AttributeWinners winnerRecord(Map<String, Object> value) {
    Map<String, String> owners = new java.util.LinkedHashMap<>();
    object(value.get("owners")).forEach((name, owner) -> owners.put(name, (String) owner));
    return new AttributeWinners(
        Element.fromMap(object(value.get("element"))),
        owners,
        Math.toIntExact(exactLong(value.get("contributor_count"))));
  }

  private static Map<String, Object> object(Object value) {
    if (!(value instanceof Map<?, ?> map)) {
      throw new IllegalArgumentException("expected CBOR object");
    }
    Map<String, Object> result = new java.util.LinkedHashMap<>();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (!(entry.getKey() instanceof String key)) {
        throw new IllegalArgumentException("expected string object key");
      }
      result.put(key, entry.getValue());
    }
    return result;
  }

  private static long exactLong(Object value) {
    if (value instanceof Byte
        || value instanceof Short
        || value instanceof Integer
        || value instanceof Long) return ((Number) value).longValue();
    if (value instanceof BigInteger integer) return integer.longValueExact();
    throw new IllegalArgumentException("expected exact integer");
  }

  @Override
  public boolean isImmutableType() {
    return true;
  }

  @Override
  public TypeSerializer<T> duplicate() {
    return this;
  }

  @Override
  public T createInstance() {
    return null;
  }

  @Override
  public T copy(T value) {
    return value;
  }

  @Override
  public T copy(T value, T reuse) {
    return value;
  }

  @Override
  public int getLength() {
    return -1;
  }

  @Override
  public void serialize(T value, DataOutputView output) throws IOException {
    byte[] bytes = MAPPER.writeValueAsBytes(fields(value));
    if (bytes.length > MAX_FRAME_BYTES)
      throw new IOException("graph state exceeds maximum frame size");
    output.writeByte(FORMAT_VERSION);
    output.writeInt(bytes.length);
    output.write(bytes);
  }

  @Override
  public T deserialize(DataInputView input) throws IOException {
    try {
      int length = binaryLength(input);
      byte[] bytes = new byte[length];
      input.readFully(bytes);
      return record(MAPPER.readValue(bytes, OBJECT));
    } catch (IllegalArgumentException exception) {
      throw new IOException("invalid " + kind + " graph state", exception);
    }
  }

  @Override
  public T deserialize(T reuse, DataInputView input) throws IOException {
    return deserialize(input);
  }

  @Override
  public void copy(DataInputView input, DataOutputView output) throws IOException {
    int length = binaryLength(input);
    output.writeByte(FORMAT_VERSION);
    output.writeInt(length);
    byte[] buffer = new byte[Math.min(length, 8192)];
    for (int remaining = length; remaining > 0; ) {
      int count = Math.min(remaining, buffer.length);
      input.readFully(buffer, 0, count);
      output.write(buffer, 0, count);
      remaining -= count;
    }
  }

  private static int binaryLength(DataInputView input) throws IOException {
    int version = input.readUnsignedByte();
    if (version != FORMAT_VERSION)
      throw new IOException("unsupported graph state format " + version);
    int length = input.readInt();
    if (length < 0 || length > MAX_FRAME_BYTES)
      throw new IOException("invalid graph state frame length");
    return length;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof StateSerializer<?> value && value.kind == kind;
  }

  @Override
  public int hashCode() {
    return kind.hashCode();
  }

  @Override
  public TypeSerializerSnapshot<T> snapshotConfiguration() {
    return new Snapshot<>(kind);
  }

  public static final class Snapshot<T> implements TypeSerializerSnapshot<T> {
    private Kind kind;

    public Snapshot() {}

    Snapshot(Kind kind) {
      this.kind = kind;
    }

    @Override
    public int getCurrentVersion() {
      return FORMAT_VERSION;
    }

    @Override
    public void writeSnapshot(DataOutputView output) throws IOException {
      output.writeUTF(kind.name());
    }

    @Override
    public void readSnapshot(int version, DataInputView input, ClassLoader loader)
        throws IOException {
      if (version != FORMAT_VERSION)
        throw new IOException("unsupported graph serializer snapshot " + version);
      try {
        kind = Kind.valueOf(input.readUTF());
      } catch (IllegalArgumentException exception) {
        throw new IOException("unknown graph state kind", exception);
      }
    }

    @Override
    public TypeSerializer<T> restoreSerializer() {
      return new StateSerializer<>(kind);
    }

    @Override
    public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
        TypeSerializerSnapshot<T> old) {
      if (old instanceof Snapshot<?> snapshot && snapshot.kind == kind) {
        return TypeSerializerSchemaCompatibility.compatibleAsIs();
      }
      return TypeSerializerSchemaCompatibility.incompatible();
    }
  }
}
