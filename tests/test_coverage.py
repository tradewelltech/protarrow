"""
These tests are here to get the coverage to 100%
They cover scenarios that don't make sense IRL.
"""

import dataclasses
import datetime
from typing import Optional

import pyarrow as pa
import pytest
from google.protobuf.descriptor import Descriptor, EnumDescriptor, FieldDescriptor
from google.protobuf.wrappers_pb2 import BoolValue, DoubleValue
from google.type.date_pb2 import Date

import protarrow
from protarrow import cast_record_batch
from protarrow.arrow_to_proto import (
    MapItemAssigner,
    OffsetsIterator,
    OffsetToSize,
    OptionalNestedIterable,
    PlainAssigner,
    RepeatedNestedIterable,
    _extract_array_messages,
    _extract_map_field,
    _extract_record_batch_messages,
    convert_scalar,
)
from protarrow.cast_to_proto import get_arrow_default_value
from protarrow.message_extractor import (
    MapAsListConverterAdapter,
    MapConverterAdapter,
    NullableConverterAdapter,
    RepeatedConverterAdapter,
    StructScalarConverter,
)
from protarrow.proto_to_arrow import (
    NestedIterable,
    _get_converter,
    field_descriptor_to_data_type,
    get_enum_converter,
)
from protarrow_protos.bench_pb2 import (
    ExampleMessage,
    NestedExampleMessage,
    SuperNestedExampleMessage,
)
from protarrow_protos.example_pb2 import TestEnum, WithEnum


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


def test_map_as_list_converter_adapter():
    list_type = pa.list_(pa.struct([("key", pa.int32()), ("value", pa.float64())]))
    map_field = ExampleMessage.DESCRIPTOR.fields_by_name["double_int32_map"]
    map_converter_adapter = MapAsListConverterAdapter(
        list_type=list_type,
        key_descriptor=map_field.message_type.fields_by_name["key"],
        value_descriptor=map_field.message_type.fields_by_name["value"],
    )
    assert map_converter_adapter(pa.scalar([(123, 1.0)], list_type)) == {123: 1.0}
    assert map_converter_adapter(pa.scalar([], list_type)) == {}
    assert map_converter_adapter(pa.scalar(None, list_type)) == {}


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
        None,
    ]


def test_map_item_assigner():
    array = pa.array(
        [[("hello", True), ("world", None)], None, ()],
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


def test_offset_to_size():
    assert list(OffsetToSize(pa.array([0, 5, 10]))) == [5, 5]
    assert list(OffsetToSize(pa.array([5, 10]))) == [5]
    assert list(OffsetToSize(pa.array([], pa.int32()))) == []
    assert list(OffsetToSize(pa.array([1]))) == []


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

    assert (
        get_enum_converter(pa.dictionary(pa.int32(), pa.binary()), enum_descriptor)(10)
        == b"UNKNOWN_EXAMPLE_ENUM"
    )

    assert (
        get_enum_converter(pa.dictionary(pa.int32(), pa.string()), enum_descriptor)(10)
        == "UNKNOWN_EXAMPLE_ENUM"
    )

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


def test_struct_scalar_converter():
    struct_type = pa.struct([pa.field("double_value", pa.float64())])
    struct_scalar_converter = StructScalarConverter(
        struct_type, ExampleMessage.DESCRIPTOR
    )
    assert struct_scalar_converter(
        pa.scalar([("double_value", 1.0)], struct_type)
    ) == ExampleMessage(double_value=1.0)
    assert (
        struct_scalar_converter(pa.scalar([("double_value", None)], struct_type))
        == ExampleMessage()
    )


def test_plain_assigner_not_nullable():
    messages = [ExampleMessage(), ExampleMessage()]
    plain_assigner = PlainAssigner(
        messages=messages,
        field_descriptor=ExampleMessage.DESCRIPTOR.fields_by_name["double_value"],
        arrow_type=pa.float64(),
    )
    for a, v in zip(plain_assigner, pa.array([1.0, None], pa.float64())):
        a(v)
    assert messages == [ExampleMessage(double_value=1.0), ExampleMessage()]


def test_plain_assigner_nullable():
    messages = [
        ExampleMessage(),
        ExampleMessage(),
        ExampleMessage(),
    ]
    plain_assigner = PlainAssigner(
        messages=messages,
        field_descriptor=ExampleMessage.DESCRIPTOR.fields_by_name[
            "wrapped_double_value"
        ],
        arrow_type=pa.float64(),
    )
    for a, v in zip(plain_assigner, pa.array([1.0, 0.0, None], pa.float64())):
        a(v)
    assert messages == [
        ExampleMessage(wrapped_double_value=DoubleValue(value=1.0)),
        ExampleMessage(wrapped_double_value=DoubleValue(value=0.0)),
        ExampleMessage(),
    ]


def test_missing_column():
    messages = [ExampleMessage(), ExampleMessage()]
    _extract_record_batch_messages(
        pa.table({"double_value": pa.array([1.0, 2.0])}).to_batches()[0],
        ExampleMessage.DESCRIPTOR,
        messages,
    )
    assert messages == [
        ExampleMessage(double_value=1.0),
        ExampleMessage(double_value=2.0),
    ]


def test_missing_column_in_struct():
    messages = [ExampleMessage(), ExampleMessage()]
    struct_type = pa.struct([pa.field("double_value", pa.float64())])
    _extract_array_messages(
        pa.array([[("double_value", 1.0)], [("double_value", 2.0)]], struct_type),
        ExampleMessage.DESCRIPTOR,
        messages,
    )
    assert messages == [
        ExampleMessage(double_value=1.0),
        ExampleMessage(double_value=2.0),
    ]


def test_missing_map_field():
    messages = [NestedExampleMessage(example_message_string_map={})]
    struct_type = pa.struct([pa.field("double_value", pa.float64())])

    map_array = pa.array(
        [
            [
                ("message_1", {"double_value": 1.0}),
                ("message_2", {"double_value": 2.0}),
            ]
        ],
        pa.map_(pa.string(), struct_type),
    )
    _extract_map_field(
        map_array,
        NestedExampleMessage.DESCRIPTOR.fields_by_name["example_message_string_map"],
        messages,
    )
    assert messages == [
        NestedExampleMessage(
            example_message_string_map={
                "message_1": ExampleMessage(double_value=1.0),
                "message_2": ExampleMessage(double_value=2.0),
            }
        )
    ]


def test_missing_enum_from_proto():
    message = ExampleMessage()
    message.example_enum_value = 150  # does not exist
    assert protarrow.messages_to_table([message], ExampleMessage)[
        "example_enum_value"
    ].to_pylist() == [150]

    assert protarrow.messages_to_table(
        [message], ExampleMessage, protarrow.ProtarrowConfig(enum_type=pa.string())
    )["example_enum_value"].to_pylist() == ["UNKNOWN_EXAMPLE_ENUM"]

    assert protarrow.messages_to_table(
        [message], ExampleMessage, protarrow.ProtarrowConfig(enum_type=pa.binary())
    )["example_enum_value"].to_pylist() == [b"UNKNOWN_EXAMPLE_ENUM"]


def test_nested_missing_values():
    """
    Check for cases where nested messages are null.

    When a nested message is null, the arrow arrays of their underlying fields
    """
    source_messages = [
        NestedExampleMessage(),
        NestedExampleMessage(example_message=ExampleMessage(double_value=1.0)),
    ]
    table = protarrow.messages_to_table(
        source_messages,
        NestedExampleMessage,
    )
    messages_back = protarrow.table_to_messages(table, NestedExampleMessage)
    assert messages_back == source_messages


def test_missing_temporal_nested_value():
    for temporal_name, temporal_value in {
        "time_of_day_value": datetime.time(hour=1),
        "timestamp_value": datetime.datetime.now(),
        "date_value": datetime.date.today(),
    }.items():
        table = pa.table(
            {
                "example_message": pa.array(
                    [
                        None,
                        {temporal_name: temporal_value},
                    ]
                )
            }
        )
        protarrow.table_to_messages(table, NestedExampleMessage)
        assert table.flatten()[
            f"example_message.{temporal_name}"
        ].is_valid().to_pylist() == [False, True]


def test_missing_nested_column():
    table = pa.table(
        {"repeated_nested_example_message": pa.nulls(10, pa.list_(pa.struct([])))}
    )
    protarrow.cast_table(table, SuperNestedExampleMessage, protarrow.ProtarrowConfig())
    protarrow.table_to_messages(table, SuperNestedExampleMessage)


def test_misaligned_nested_iterable():
    """Special case where the validity mask isn't aligned wit the nested values"""
    iterable = OptionalNestedIterable(
        [None, NestedExampleMessage(), None],
        NestedExampleMessage.DESCRIPTOR.fields_by_name["example_message"],
        [pa.scalar(False), pa.scalar(False), pa.scalar(True)],
    )
    nested = list(iterable)
    assert nested == [None, None, None]


def test_misaligned_nested_iterable_prime():
    """Special case where the validity mask isn't aligned wit the nested values"""
    iterable = OptionalNestedIterable(
        [None, NestedExampleMessage(), None],
        NestedExampleMessage.DESCRIPTOR.fields_by_name["example_message"],
        [pa.scalar(False), pa.scalar(False), pa.scalar(True)],
    )
    iterable.prime()
    nested = list(iterable)
    assert nested == [None, None, None]


def test_missing_parent_repeated_nested_iterable():
    iterable = RepeatedNestedIterable(
        [
            None,
            NestedExampleMessage(),
            NestedExampleMessage(
                repeated_example_message=[ExampleMessage(string_value="hello")]
            ),
        ],
        NestedExampleMessage.DESCRIPTOR.fields_by_name["repeated_example_message"],
    )
    assert list(iterable) == [ExampleMessage(string_value="hello")]


def test_coverage_offset_iterator():
    offsets = OffsetsIterator(pa.array([]))
    assert list(offsets) == []


def test_nested_iterable():
    nested_iterable = NestedIterable([], lambda x: x.foo)
    assert len(nested_iterable) == 0


@pytest.mark.parametrize(
    ("config", "expected"),
    [
        (protarrow.ProtarrowConfig(enum_type=pa.int32()), [0, 1, 2, 0]),
        # [(protarrow.ProtarrowConfig(enum_type=pa.int64()), [0, 1, 2, 0])],
        (
            protarrow.ProtarrowConfig(enum_type=pa.string()),
            ["UNKNOWN_TEST_ENUM", "TEST_ENUM_1", "TEST_ENUM_2", "UNKNOWN_TEST_ENUM"],
        ),
        (
            protarrow.ProtarrowConfig(
                string_type=pa.large_string(), enum_type=pa.large_string()
            ),
            ["UNKNOWN_TEST_ENUM", "TEST_ENUM_1", "TEST_ENUM_2", "UNKNOWN_TEST_ENUM"],
        ),
        (
            protarrow.ProtarrowConfig(enum_type=pa.binary()),
            [
                b"UNKNOWN_TEST_ENUM",
                b"TEST_ENUM_1",
                b"TEST_ENUM_2",
                b"UNKNOWN_TEST_ENUM",
            ],
        ),
        (
            protarrow.ProtarrowConfig(
                binary_type=pa.large_binary(), enum_type=pa.large_binary()
            ),
            [
                b"UNKNOWN_TEST_ENUM",
                b"TEST_ENUM_1",
                b"TEST_ENUM_2",
                b"UNKNOWN_TEST_ENUM",
            ],
        ),
        (
            protarrow.ProtarrowConfig(enum_type=pa.dictionary(pa.int32(), pa.string())),
            ["UNKNOWN_TEST_ENUM", "TEST_ENUM_1", "TEST_ENUM_2", "UNKNOWN_TEST_ENUM"],
        ),
        (
            protarrow.ProtarrowConfig(enum_type=pa.dictionary(pa.int32(), pa.binary())),
            [
                b"UNKNOWN_TEST_ENUM",
                b"TEST_ENUM_1",
                b"TEST_ENUM_2",
                b"UNKNOWN_TEST_ENUM",
            ],
        ),
        (
            protarrow.ProtarrowConfig(
                enum_type=pa.dictionary(pa.int32(), pa.string()),
                string_type=pa.large_string(),
            ),
            ["UNKNOWN_TEST_ENUM", "TEST_ENUM_1", "TEST_ENUM_2", "UNKNOWN_TEST_ENUM"],
        ),
        (
            protarrow.ProtarrowConfig(
                enum_type=pa.dictionary(pa.int32(), pa.binary()),
                binary_type=pa.large_binary(),
            ),
            [
                b"UNKNOWN_TEST_ENUM",
                b"TEST_ENUM_1",
                b"TEST_ENUM_2",
                b"UNKNOWN_TEST_ENUM",
            ],
        ),
    ],
    ids=lambda _: None,
)
def test_enum_config(config, expected):
    messages = [
        WithEnum(),
        WithEnum(test_enum=TestEnum.TEST_ENUM_1),
        WithEnum(test_enum=TestEnum.TEST_ENUM_2),
        WithEnum(test_enum=TestEnum.UNKNOWN_TEST_ENUM),
    ]

    record_batch = protarrow.messages_to_record_batch(messages, WithEnum, config)

    casted = cast_record_batch(record_batch, WithEnum, config)
    assert casted == record_batch
    assert cast_record_batch(record_batch[:0], WithEnum, config) == record_batch[:0]

    assert record_batch["test_enum"] == pa.array(expected, config.enum_type)

    messages_back = protarrow.record_batch_to_messages(record_batch, WithEnum)
    assert messages_back == messages


def test_arrow_missing_function():
    pa.array([], pa.dictionary(pa.int32(), pa.string()))
    pa.array([], pa.dictionary(pa.int32(), pa.binary()))

    with pytest.raises(
        pa.ArrowNotImplementedError,
        match="DictionaryArray converter for type"
        r" dictionary\<values=large_string, indices=int32, ordered=0\> not implemented",
    ):
        pa.array([], pa.dictionary(pa.int32(), pa.large_string()))

    with pytest.raises(
        pa.ArrowNotImplementedError,
        match="DictionaryArray converter for type"
        r" dictionary\<values=large_binary, indices=int32, ordered=0\> not implemented",
    ):
        pa.array([], pa.dictionary(pa.int32(), pa.large_binary()))


class TestProtarrowConfigValidation:
    def test_invalid_enum_type(self):
        with pytest.raises(ValueError, match="Unsupported enum_type"):
            protarrow.ProtarrowConfig(enum_type=pa.float64())

    def test_invalid_string_type(self):
        with pytest.raises(ValueError, match="Unsupported string_type"):
            protarrow.ProtarrowConfig(string_type=pa.binary())

    def test_invalid_binary_type(self):
        with pytest.raises(ValueError, match="Unsupported binary_type"):
            protarrow.ProtarrowConfig(binary_type=pa.string())

    def test_invalid_list_array_type(self):
        with pytest.raises(ValueError, match="Unsupported list_array_type"):
            protarrow.ProtarrowConfig(list_array_type=pa.MapArray)

    def test_invalid_field_number_key(self):
        with pytest.raises(TypeError, match="field_number_key must be bytes or None"):
            protarrow.ProtarrowConfig(field_number_key="not_bytes")

    def test_string_enum_mismatch(self):
        with pytest.raises(ValueError, match="does not match string_type"):
            protarrow.ProtarrowConfig(
                enum_type=pa.large_string(), string_type=pa.string()
            )

    def test_binary_enum_mismatch(self):
        with pytest.raises(ValueError, match="does not match binary_type"):
            protarrow.ProtarrowConfig(
                enum_type=pa.large_binary(), binary_type=pa.binary()
            )

    def test_dict_string_enum_with_large_string(self):
        config = protarrow.ProtarrowConfig(
            enum_type=pa.dictionary(pa.int32(), pa.string()),
            string_type=pa.large_string(),
        )
        assert config.enum_type == pa.dictionary(pa.int32(), pa.string())

    def test_dict_binary_enum_with_large_binary(self):
        config = protarrow.ProtarrowConfig(
            enum_type=pa.dictionary(pa.int32(), pa.binary()),
            binary_type=pa.large_binary(),
        )
        assert config.enum_type == pa.dictionary(pa.int32(), pa.binary())


def test_date_behavior():
    assert pa.scalar(0, pa.date32()).as_py() == datetime.date(1970, 1, 1)
    assert pa.scalar(-1, pa.date32()).as_py() == datetime.date(1969, 12, 31)
    assert pa.scalar(-719162, pa.date32()).as_py() == datetime.date(1, 1, 1)
    with pytest.raises(OverflowError, match=r"date value out of range"):
        assert pa.scalar(-719163, pa.date32()).as_py()

    assert datetime.date(1970, 1, 1).toordinal() == 719163
    assert datetime.date.min.toordinal() == 1
    assert datetime.date.max.toordinal() == 3652059

    assert datetime.date.fromordinal(1) == datetime.date.min
    assert datetime.date.fromordinal(3652059) == datetime.date.max


def test_bad_date():
    default_date = Date()
    min_date = Date(year=1, month=1, day=1)

    assert default_date != min_date


def test_can_pass_min_max_date():
    default_date = Date()
    bad_date = Date(year=0, month=1, day=1)
    min_date = Date(year=1, month=1, day=1)
    max_date = Date(year=9999, month=12, day=31)

    messages = [
        ExampleMessage(date_value=default_date),
        ExampleMessage(date_value=bad_date),
        ExampleMessage(date_value=min_date),
        ExampleMessage(date_value=max_date),
    ]
    table = protarrow.messages_to_table(messages, ExampleMessage)
    assert table["date_value"] == pa.chunked_array(
        pa.array(
            [
                -719163,
                -719163,
                -719162,
                2932896,
            ],
            type=pa.date32(),
        )
    )

    messages_back = protarrow.table_to_messages(table, ExampleMessage)
    assert messages_back == [
        ExampleMessage(date_value=default_date),
        ExampleMessage(date_value=default_date),  # This changed
        ExampleMessage(date_value=min_date),
        ExampleMessage(date_value=max_date),
    ]
