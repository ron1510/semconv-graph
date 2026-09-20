package io.extendedotel.flink;

import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/** Explicit contribution transport; Java record reflection/Kryo is never used. */
public final class ContributionTypeInformation extends TypeInformation<GraphModel.Contribution> {
  public static final ContributionTypeInformation INSTANCE = new ContributionTypeInformation();

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
  public Class<GraphModel.Contribution> getTypeClass() {
    return GraphModel.Contribution.class;
  }

  @Override
  public boolean isKeyType() {
    return false;
  }

  @Override
  public TypeSerializer<GraphModel.Contribution> createSerializer(SerializerConfig config) {
    return new Serializer();
  }

  @Override
  public String toString() {
    return "GraphContributionCborV2";
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof ContributionTypeInformation;
  }

  @Override
  public int hashCode() {
    return ContributionTypeInformation.class.hashCode();
  }

  @Override
  public boolean canEqual(Object other) {
    return other instanceof ContributionTypeInformation;
  }

  public static final class Serializer extends StateSerializer<GraphModel.Contribution> {
    public Serializer() {
      super(Kind.CONTRIBUTION);
    }
  }
}
