"""
These tests are here to get the coverage to 100%
They cover scenarios that don't make sense IRL.
"""
import dataclasses
from typing import Optional

import pyarrow as pa
import pytest
from google.protobuf.descriptor import Descriptor, EnumDescriptor, FieldDescriptor
from google.protobuf.wrappers_pb2 import BoolValue, DoubleValue

import protarrow
from protarrow.arrow_to_proto import (
    MapItemAssigner,
    OffsetToSize,
    OptionalNestedIterable,
    convert_scalar,
)
from protarrow.cast_to_proto import get_arrow_default_value
from protarrow.message_extractor import (
    MapConverterAdapter,
    NullableConverterAdapter,
    RepeatedConverterAdapter,
)
from protarrow.proto_to_arrow import (
    _get_converter,
    field_descriptor_to_data_type,
    get_enum_converter,
)
from protarrow_protos.bench_pb2 import ExampleMessage, NestedExampleMessage


def test_map_converter_adapter():
    map_type = pa.map_(pa.int32(), pa.float64())
    map_field = ExampleMessage.DESCRIPTOR.fields_by_name["double_int32_map"]
    map_converter_adapter = MapConverterAdapter(
        map_type=map_type,
        key_descriptor=map_field.message_type.fields_by_name["key"],
        value_descriptor=map_field.message_type.fields_by_name["value"],
    )
    assert map_converter_adapter(pa.scalar([(123, 1.0)], map_type)) == {123: 1.0}
    assert map_converter_adapter(pa.scalar([], map_type)) == {}
    assert map_converter_adapter(pa.scalar(None, map_type)) == {}


def test_nullable_converter_adapter():
    nullable_converter_adapter = NullableConverterAdapter(convert_scalar, DoubleValue)
    assert nullable_converter_adapter(pa.scalar(1.0, pa.float64())) == DoubleValue(
        value=1.0
    )
    assert nullable_converter_adapter(pa.scalar(None, pa.float64())) is None


def test_repeated_converter_adapter():
    repeated_converter_adapter = RepeatedConverterAdapter(convert_scalar)
    assert repeated_converter_adapter(pa.scalar([1, 2, 3], pa.list_(pa.int32()))) == [
        1,
        2,
        3,
    ]
    assert repeated_converter_adapter(pa.scalar([], pa.list_(pa.int32()))) == []
    assert repeated_converter_adapter(pa.scalar(None, pa.list_(pa.int32()))) == []


def test_optional_nested_iterable():
    optional_nested_iterable = OptionalNestedIterable(
        [
            NestedExampleMessage(),
            NestedExampleMessage(example_message=ExampleMessage(int32_value=1)),
            None,
        ],
        NestedExampleMessage.DESCRIPTOR.fields_by_name["example_message"],
        pa.array([True, True, False], pa.bool_()),
    )

    assert list(optional_nested_iterable) == [
        ExampleMessage(),
        ExampleMessage(int32_value=1),
        ExampleMessage(),
    ]


def test_map_item_assigner():
    array = pa.array(
        [[("hello", True), ("world", None)], None, tuple()],
        pa.map_(pa.string(), pa.bool_()),
    )

    messages = [ExampleMessage(), ExampleMessage(), ExampleMessage()]
    map_item_assigner = MapItemAssigner(
        messages,
        field_descriptor=ExampleMessage.DESCRIPTOR.fields_by_name[
            "wrapped_bool_string_map"
        ],
        key_arrow_type=pa.string(),
        value_arrow_type=pa.bool_(),
        sizes=OffsetToSize(array.offsets),
    )

    for a, k, v in zip(map_item_assigner, array.keys, array.items):
        a(k, v)
    assert messages == [
        ExampleMessage(
            wrapped_bool_string_map={
                "hello": BoolValue(value=True),
                "world": BoolValue(value=False),
            }
        ),
        ExampleMessage(),
        ExampleMessage(),
    ]


def test_get_enum_converter():
    enum_descriptor = ExampleMessage.DESCRIPTOR.fields_by_name[
        "example_enum_value"
    ].enum_type

    assert get_enum_converter(pa.int32(), enum_descriptor)(1) == 1
    assert get_enum_converter(pa.int32(), enum_descriptor)(10) == 10
    assert get_enum_converter(pa.string(), enum_descriptor)(1) == "EXAMPLE_ENUM_1"
    assert (
        get_enum_converter(pa.dictionary(pa.int32(), pa.string()), enum_descriptor)(1)
        == "EXAMPLE_ENUM_1"
    )
    assert (
        get_enum_converter(pa.binary(), enum_descriptor)(0) == b"UNKNOWN_EXAMPLE_ENUM"
    )
    assert (
        get_enum_converter(pa.dictionary(pa.int32(), pa.binary()), enum_descriptor)(1)
        == b"EXAMPLE_ENUM_1"
    )
    with pytest.raises(KeyError, match="10"):
        get_enum_converter(pa.dictionary(pa.int32(), pa.binary()), enum_descriptor)(10)

    with pytest.raises(TypeError, match="double"):
        get_enum_converter(pa.float64(), enum_descriptor)


@dataclasses.dataclass
class FakeConfig:
    enum_type: pa.DataType


def test_get_arrow_default_value():
    with pytest.raises(TypeError, match="double"):
        get_arrow_default_value(
            ExampleMessage.DESCRIPTOR.fields_by_name["example_enum_value"],
            FakeConfig(enum_type=pa.float64()),
        )


@dataclasses.dataclass(frozen=True)
class FakeDescriptor:
    name: str
    type: int
    enum_type: Optional[Descriptor] = None
    message_type: Optional[EnumDescriptor] = None


def test_field_descriptor_to_data_type():
    with pytest.raises(TypeError, match=r"Unsupported field type TYPE_GROUP for foo"):
        field_descriptor_to_data_type(
            FakeDescriptor("foo", type=FieldDescriptor.TYPE_GROUP),
            protarrow.ProtarrowConfig(),
        )


def test_get_converter():
    with pytest.raises(TypeError, match=r"Unsupported field type TYPE_GROUP for foo"):
        _get_converter(
            FakeDescriptor("foo", type=FieldDescriptor.TYPE_GROUP),
            protarrow.ProtarrowConfig(),
        )
