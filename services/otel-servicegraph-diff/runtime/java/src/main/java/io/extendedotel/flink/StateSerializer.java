package io.extendedotel.flink;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import java.io.IOException;
import java.util.Map;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/** Typed, length-framed CBOR state for the clean Java runtime. */
class StateSerializer<T> extends TypeSerializer<T> {
  enum Kind {
    CONTRIBUTOR,
    AGGREGATE,
    WINNERS,
    CONTRIBUTION,
    EVENT
  }

  private static final int FORMAT_VERSION = 2;
  private static final int MAX_FRAME_BYTES = 64 * 1024 * 1024;
  private static final ObjectMapper MAPPER =
      new ObjectMapper(new CBORFactory()).enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  private static final TypeReference<Map<String, Object>> OBJECT = new TypeReference<>() {};
  private final Kind kind;

  StateSerializer(Kind kind) {
    this.kind = kind;
  }

  static StateSerializer<GraphModel.Snapshot> contributor() {
    return new StateSerializer<>(Kind.CONTRIBUTOR);
  }

  static StateSerializer<GraphModel.Aggregate> aggregate() {
    return new StateSerializer<>(Kind.AGGREGATE);
  }

  static StateSerializer<AttributeWinners> winners() {
    return new StateSerializer<>(Kind.WINNERS);
  }

  private Map<String, Object> fields(T value) {
    return switch (kind) {
      case CONTRIBUTOR -> ((GraphModel.Snapshot) value).toMap();
      case AGGREGATE -> ((GraphModel.Aggregate) value).toMap();
      case WINNERS -> ((AttributeWinners) value).toMap();
      case CONTRIBUTION -> ((GraphModel.Contribution) value).toMap();
      case EVENT -> ((GraphModel.Event) value).toMap();
    };
  }

  @SuppressWarnings(
      "unchecked") // Kind is fixed by typed factories and persisted in the serializer snapshot.
  private T record(Map<String, Object> fields) {
    return (T)
        switch (kind) {
          case CONTRIBUTOR -> GraphModel.Snapshot.fromMap(fields);
          case AGGREGATE -> GraphModel.Aggregate.fromMap(fields);
          case WINNERS -> AttributeWinners.fromMap(fields);
          case CONTRIBUTION -> GraphModel.Contribution.fromMap(fields);
          case EVENT -> GraphModel.Event.fromMap(fields);
        };
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
