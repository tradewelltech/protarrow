from typing import Any, Callable, Dict, Generic, List, Type, TypeVar

import pyarrow as pa
from google.protobuf.descriptor import Descriptor, FieldDescriptor
from google.protobuf.message import Message

from protarrow.arrow_to_proto import NULLABLE_TYPES, get_converter
from protarrow.proto_to_arrow import get_map_descriptors, is_map

M = TypeVar("M", bound=Message)


class StructScalarConverter:
    def __init__(self, struct_type: pa.StructType, descriptor: Descriptor):
        self._converters = {
            field_descriptor.name: get_field_converter(
                struct_type.field(field_descriptor.name), field_descriptor
            )
            for field_descriptor in descriptor.fields
            if struct_type.get_field_index(field_descriptor.name) >= 0
        }
        self._message_type = descriptor._concrete_class

    def __call__(self, scalar: pa.StructScalar) -> Message:
        results = {}
        for field_name, field_extractor in self._converters.items():
            field_scalar = scalar[field_name]
            if field_scalar.is_valid:
                results[field_name] = field_extractor(field_scalar)
        return self._message_type(**results)


class RepeatedConverterAdapter:
    def __init__(self, converter: Callable[[pa.Scalar], Any]):
        self._converter = converter

    def __call__(self, scalar: pa.ListScalar) -> List[Any]:
        if scalar.is_valid:
            return [self._converter(scalar_item) for scalar_item in scalar]
        else:
            return []


class MapConverterAdapter:
    def __init__(
        self,
        map_type: pa.MapType,
        key_descriptor: FieldDescriptor,
        value_descriptor: FieldDescriptor,
    ):
        self._key_converter = get_flat_field_converter(
            map_type.key_field.type, key_descriptor
        )
        self._value_converter = get_flat_field_converter(
            map_type.item_field.type, value_descriptor
        )

    def __call__(self, scalar: pa.MapScalar) -> Dict[Any, Any]:
        if scalar.is_valid:
            return {
                self._key_converter(k): self._value_converter(v)
                for k, v in zip(scalar.values.field(0), scalar.values.field(1))
            }
        else:
            return {}


class NullableConverterAdapter:
    def __init__(
        self, converter: Callable[[pa.Scalar], Any], message_type: Type[Message]
    ):
        self._converter = converter
        self._message_type = message_type

    def __call__(self, scalar: pa.Scalar) -> Any:
        if scalar.is_valid:
            return self._message_type(value=self._converter(scalar))
        else:
            return None


def get_flat_field_converter(
    data_type: pa.DataType, field_descriptor: FieldDescriptor
) -> Callable[[pa.Scalar], Any]:
    try:
        converter = get_converter(field_descriptor, data_type)
        if field_descriptor.message_type in NULLABLE_TYPES:
            return NullableConverterAdapter(
                converter, field_descriptor.message_type._concrete_class
            )
        else:
            return converter
    except KeyError:
        return StructScalarConverter(data_type, field_descriptor.message_type)


def get_field_converter(
    field: pa.Field, field_descriptor: FieldDescriptor
) -> Callable[[pa.Scalar], Any]:
    if is_map(field_descriptor):
        key, value = get_map_descriptors(field_descriptor)
        return MapConverterAdapter(field.type, key, value)
    else:

        if field_descriptor.label == FieldDescriptor.LABEL_REPEATED:
            return RepeatedConverterAdapter(
                get_flat_field_converter(field.type.value_type, field_descriptor)
            )
        else:
            return get_flat_field_converter(field.type, field_descriptor)


class MessageExtractor(Generic[M]):
    def __init__(self, schema: pa.Schema, message_type: Type[M]):
        descriptor = message_type.DESCRIPTOR
        self._extractors = {
            schema.get_field_index(field_descriptor.name): get_field_converter(
                schema.field(field_descriptor.name), field_descriptor
            )
            for field_descriptor in descriptor.fields
            if schema.get_field_index(field_descriptor.name) >= 0
        }
        self._message_type = message_type

    def read_table_row(self, table: pa.Table, row: int) -> M:
        results = {}
        for index, converter in self._extractors.items():
            scalar = table.column(index)[row]
            if scalar.is_valid:
                results[table.schema.field(index).name] = converter(scalar)
        return self._message_type(**results)
