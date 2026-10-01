"""Runtime primitives shared by generated semantic entity classes."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Annotated, ClassVar, Self, cast

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    FiniteFloat,
    PlainSerializer,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
    ValidationError,
    model_validator,
)

from extended_otel_semconv.errors import (
    SemanticModelValidationError,
    UnknownSemanticTypeError,
)

type EntityId = str
type SemanticScalar = StrictStr | StrictInt | StrictFloat | StrictBool
type SemanticSequence = tuple[SemanticScalar, ...]
type SemanticAttributeValue = SemanticScalar | SemanticSequence
type RawAttributes = Mapping[str, object]

def _as_tuple(value: object) -> object:
    return tuple(cast(list[object], value)) if isinstance(value, list) else value


def _freeze_mapping[Value](value: Mapping[str, Value]) -> Mapping[str, Value]:
    return MappingProxyType(dict(value))


def _serialize_mapping[Value](value: Mapping[str, Value]) -> dict[str, Value]:
    return dict(value)


type StringSequence = Annotated[tuple[StrictStr, ...], BeforeValidator(_as_tuple)]
type IntegerSequence = Annotated[tuple[StrictInt, ...], BeforeValidator(_as_tuple)]
type NumberSequence = Annotated[tuple[FiniteFloat, ...], BeforeValidator(_as_tuple)]
type BooleanSequence = Annotated[tuple[StrictBool, ...], BeforeValidator(_as_tuple)]

type FrozenStringMap = Annotated[
    Mapping[str, StrictStr],
    AfterValidator(_freeze_mapping),
    PlainSerializer(_serialize_mapping),
]
type FrozenIntegerMap = Annotated[
    Mapping[str, StrictInt],
    AfterValidator(_freeze_mapping),
    PlainSerializer(_serialize_mapping),
]
type FrozenNumberMap = Annotated[
    Mapping[str, FiniteFloat],
    AfterValidator(_freeze_mapping),
    PlainSerializer(_serialize_mapping),
]
type FrozenBooleanMap = Annotated[
    Mapping[str, StrictBool],
    AfterValidator(_freeze_mapping),
    PlainSerializer(_serialize_mapping),
]
type FrozenStringSequenceMap = Annotated[
    Mapping[str, StringSequence],
    AfterValidator(_freeze_mapping),
    PlainSerializer(_serialize_mapping),
]
type FrozenIntegerSequenceMap = Annotated[
    Mapping[str, IntegerSequence],
    AfterValidator(_freeze_mapping),
    PlainSerializer(_serialize_mapping),
]
type FrozenNumberSequenceMap = Annotated[
    Mapping[str, NumberSequence],
    AfterValidator(_freeze_mapping),
    PlainSerializer(_serialize_mapping),
]
type FrozenBooleanSequenceMap = Annotated[
    Mapping[str, BooleanSequence],
    AfterValidator(_freeze_mapping),
    PlainSerializer(_serialize_mapping),
]


class SemanticEntity(BaseModel):
    model_config = ConfigDict(
        allow_inf_nan=False,
        extra="forbid",
        frozen=True,
        populate_by_name=True,
        strict=True,
    )

    entity_type: ClassVar[str]
    identity_fields: ClassVar[tuple[str, ...]]
    template_fields: ClassVar[tuple[str, ...]] = ()
    element_id: EntityId = Field(min_length=1)

    @model_validator(mode="after")
    def freeze_containers(self) -> Self:
        for field_name in type(self).model_fields:
            object.__setattr__(self, field_name, _freeze_value(getattr(self, field_name)))
        return self

    @property
    def entity_id(self) -> EntityId:
        return self.element_id

    def semantic_attributes(self) -> dict[str, object]:
        attributes: dict[str, object] = {}
        for field_name, field in type(self).model_fields.items():
            if field_name == "element_id":
                continue
            value = getattr(self, field_name)
            if value is None:
                continue
            alias = field.alias or field_name
            if alias in self.template_fields:
                for suffix, template_value in value.items():
                    attributes[f"{alias}.{suffix}"] = template_value
            else:
                attributes[alias] = value
        return attributes

def semantic_entity_from_data(
    entity_type: str,
    element_id: str,
    attributes: RawAttributes,
) -> SemanticEntity:
    from extended_otel_semconv.generated import ENTITY_MODELS

    model = ENTITY_MODELS.get(entity_type)
    if model is None:
        raise UnknownSemanticTypeError(f"no generated semantic entity model for {entity_type!r}")
    values: dict[str, object] = {"element_id": element_id}
    for field_name, field in model.model_fields.items():
        if field_name == "element_id":
            continue
        alias = field.alias or field_name
        if alias in model.template_fields:
            template_values = _template_values(attributes, alias)
            if template_values:
                values[alias] = template_values
        elif alias in attributes:
            values[alias] = attributes[alias]
    try:
        return model.model_validate(values)
    except ValidationError as error:
        raise SemanticModelValidationError(f"invalid {model.__name__} attributes: {error}") from error


def _template_values(attributes: RawAttributes, prefix: str) -> dict[str, object]:
    dotted_prefix = f"{prefix}."
    return {
        key.removeprefix(dotted_prefix): value
        for key, value in attributes.items()
        if key.startswith(dotted_prefix) and key != dotted_prefix
    }


def _freeze_value(value: object) -> object:
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        return MappingProxyType({str(key): _freeze_value(item) for key, item in mapping.items()})
    if isinstance(value, list | tuple):
        sequence = cast(list[object] | tuple[object, ...], value)
        return tuple(_freeze_value(item) for item in sequence)
    return value
