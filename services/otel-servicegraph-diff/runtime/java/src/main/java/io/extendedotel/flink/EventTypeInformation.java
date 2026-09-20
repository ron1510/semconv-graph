package io.extendedotel.flink;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/** Explicit event transport; records and nested JSON attributes need no reflective serializer. */
public final class EventTypeInformation extends TypeInformation<GraphModel.Event> {
  public static final EventTypeInformation INSTANCE = new EventTypeInformation();

  @Override
  public boolean isBasicType() {
    return false;
  }

  @Override
  public boolean isTupleType() {
    return false;
  }

  @Override
  public int getArity() {
    return 1;
  }

  @Override
  public int getTotalFields() {
    return 1;
  }

  @Override
  public Class<GraphModel.Event> getTypeClass() {
    return GraphModel.Event.class;
  }

  @Override
  public boolean isKeyType() {
    return false;
  }

  @Override
  public TypeSerializer<GraphModel.Event> createSerializer(SerializerConfig config) {
    return new Serializer();
  }

  @Override
  public String toString() {
    return "GraphEventCborV2";
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof EventTypeInformation;
  }

  @Override
  public int hashCode() {
    return EventTypeInformation.class.hashCode();
  }

  @Override
  public boolean canEqual(Object other) {
    return other instanceof EventTypeInformation;
  }

  public static final class Serializer extends StateSerializer<GraphModel.Event> {
    public Serializer() {
      super(Kind.EVENT);
    }
  }
}
