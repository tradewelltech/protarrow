import datetime
import pathlib
from typing import Any, Iterable, List, Type

import pyarrow as pa
import pytest
from google.protobuf.json_format import Parse
from google.protobuf.message import Message
from google.protobuf.timestamp_pb2 import Timestamp
from google.type.timeofday_pb2 import TimeOfDay

import protarrow
from protarrow.arrow_to_proto import (
    create_enum_converter,
    is_custom_field,
    table_to_messages,
)
from protarrow.cast_to_proto import cast_table, get_arrow_default_value
from protarrow.common import M, ProtarrowConfig
from protarrow.proto_to_arrow import (
    NestedIterable,
    _repeated_proto_to_array,
    message_type_to_schema,
    message_type_to_struct_type,
    messages_to_record_batch,
    messages_to_table,
)
from protarrow_protos.bench_pb2 import ExampleMessage, NestedExampleMessage
from tests.random_generator import generate_messages, random_date, truncate_nanos

MESSAGES = [ExampleMessage, NestedExampleMessage]
CONFIGS = [
    ProtarrowConfig(),
    ProtarrowConfig(enum_type=pa.binary()),
    ProtarrowConfig(enum_type=pa.string()),
    ProtarrowConfig(enum_type=pa.dictionary(pa.int32(), pa.binary())),
    ProtarrowConfig(enum_type=pa.dictionary(pa.int32(), pa.string())),
    ProtarrowConfig(timestamp_type=pa.timestamp("s")),
    ProtarrowConfig(timestamp_type=pa.timestamp("ms")),
    ProtarrowConfig(timestamp_type=pa.timestamp("us")),
    ProtarrowConfig(timestamp_type=pa.timestamp("ns")),
    ProtarrowConfig(timestamp_type=pa.timestamp("s", "UTC")),
    ProtarrowConfig(timestamp_type=pa.timestamp("ms", "UTC")),
    ProtarrowConfig(timestamp_type=pa.timestamp("us", "UTC")),
    ProtarrowConfig(timestamp_type=pa.timestamp("ns", "UTC")),
    ProtarrowConfig(timestamp_type=pa.timestamp("ns", "America/New_York")),
    ProtarrowConfig(time_of_day_type=pa.time64("ns")),
    ProtarrowConfig(time_of_day_type=pa.time64("us")),
    ProtarrowConfig(list_nullable=True),
    ProtarrowConfig(map_nullable=True),
    ProtarrowConfig(map_value_nullable=True),
    ProtarrowConfig(list_value_nullable=True),
    ProtarrowConfig(list_value_name="list_value"),
    ProtarrowConfig(map_value_name="map_value"),
]


def read_proto_jsonl(path: pathlib.Path, message_type: Type[M]) -> List[M]:
    with path.open() as fp:
        return [
            Parse(line.strip(), message_type())
            for line in fp
            if line.strip() and not line.startswith("#")
        ]


@pytest.mark.parametrize("message_type", MESSAGES)
@pytest.mark.parametrize("config", CONFIGS)
def test_arrow_to_proto_empty(message_type: Type[Message], config: ProtarrowConfig):
    table = messages_to_table([], message_type, config)
    messages = table_to_messages(table, message_type)
    assert messages == []
    schema = message_type_to_schema(message_type, config)
    assert schema == table.schema


@pytest.mark.parametrize("message_type", MESSAGES)
@pytest.mark.parametrize("config", CONFIGS)
def test_message_type_to_struct_type(
    message_type: Type[Message], config: ProtarrowConfig
):
    struct_type = message_type_to_struct_type(message_type, config)
    assert isinstance(struct_type, pa.StructType)


@pytest.mark.parametrize("message_type", MESSAGES)
@pytest.mark.parametrize("config", CONFIGS)
def test_with_random(message_type: Type[Message], config: ProtarrowConfig):
    source_messages = [
        truncate_nanos(m, config.timestamp_type.unit, config.time_of_day_type.unit)
        for m in generate_messages(message_type, 10)
    ]

    table = messages_to_table(source_messages, message_type, config)
    messages_back = table_to_messages(table, message_type)
    _check_messages_same(source_messages, messages_back)


@pytest.mark.parametrize("message_type", MESSAGES)
@pytest.mark.parametrize("config", CONFIGS)
def test_with_sample_data(message_type: Type[Message], config: ProtarrowConfig):
    source_file = (
        pathlib.Path(__file__).parent / "data" / f"{message_type.DESCRIPTOR.name}.jsonl"
    )
    source_messages = [
        truncate_nanos(m, config.timestamp_type.unit, config.time_of_day_type.unit)
        for m in read_proto_jsonl(source_file, message_type)
    ]
    table = messages_to_table(source_messages, message_type, config)
    messages_back = table_to_messages(table, message_type)
    _check_messages_same(source_messages, messages_back)


def test_wrapped_type_nullable():
    expected_types = {
        "wrapped_double_value": pa.float64(),
        "wrapped_float_value": pa.float32(),
        "wrapped_int32_value": pa.int32(),
        "wrapped_int64_value": pa.int64(),
        "wrapped_uint32_value": pa.uint32(),
        "wrapped_uint64_value": pa.uint64(),
        "wrapped_bool_value": pa.bool_(),
        "wrapped_string_value": pa.string(),
        "wrapped_bytes_value": pa.binary(),
    }

    table = messages_to_table([], ExampleMessage)
    schema = table.schema
    for name, expected_type in expected_types.items():
        field = schema.field(name)
        assert field.type == expected_type
        assert field.nullable is True


def test_native_type_not_nullable():
    expected_types = {
        "double_value": pa.float64(),
        "float_value": pa.float32(),
        "int32_value": pa.int32(),
        "int64_value": pa.int64(),
        "uint32_value": pa.uint32(),
        "uint64_value": pa.uint64(),
        "bool_value": pa.bool_(),
        "string_value": pa.string(),
        "bytes_value": pa.binary(),
    }

    table = messages_to_table([], ExampleMessage)
    schema = table.schema
    for name, expected_type in expected_types.items():
        field = schema.field(name)
        assert field.type == expected_type
        assert field.nullable is False


def test_range():
    datetime.date.max - datetime.date.min
    random_date()


def test_arrow_bug_18257():
    """https://issues.apache.org/jira/browse/ARROW-18257"""
    dtype = pa.time64("ns")
    time_array = pa.array([1, 2, 3], dtype)
    assert pa.types.is_time64(time_array.type) is True
    assert isinstance(dtype, pa.Time64Type) is True
    assert isinstance(time_array.type, pa.Time64Type) is False  # Wrong
    assert isinstance(time_array.type, pa.DataType) is True  # Wrong
    assert dtype == time_array.type
    assert dtype.unit == "ns"
    with pytest.raises(
        AttributeError, match=r"'pyarrow.lib.DataType' object has no attribute 'unit'"
    ):
        # Should be able to access unit:
        time_array.type.unit


def test_arrow_bug_18264():
    """https://issues.apache.org/jira/browse/ARROW-18264"""
    time_ns = pa.array([1, 2, 3], pa.time64("ns"))
    scalar = time_ns[0]
    with pytest.raises(
        ValueError,
        match=r"Nanosecond resolution temporal type 1 is not safely convertible "
        r"to microseconds to convert to datetime.datetime. "
        r"Install pandas to return as Timestamp with nanosecond support or "
        r"access the .value attribute",
    ):
        scalar.as_py()
    with pytest.raises(
        AttributeError,
        match=r"'pyarrow.lib.Time64Scalar' object has no attribute 'value'",
    ):
        scalar.value


def test_enum_values_as_int():
    records = [
        ExampleMessage(example_enum_values=[0, 1, 0]),
        ExampleMessage(example_enum_values=[]),
        ExampleMessage(),
    ]

    array = _repeated_proto_to_array(
        NestedIterable(records, lambda x: x.example_enum_values),
        ExampleMessage.DESCRIPTOR.fields_by_name["example_enum_values"],
        ProtarrowConfig(),
    )
    assert array.to_pylist() == [[0, 1, 0], [], []]


@pytest.mark.parametrize(
    ["config", "expected"],
    [
        (ProtarrowConfig(), [[0, 1, 0], [], []]),
        (
            ProtarrowConfig(enum_type=pa.string()),
            [
                ["UNKNOWN_EXAMPLE_ENUM", "EXAMPLE_ENUM_1", "UNKNOWN_EXAMPLE_ENUM"],
                [],
                [],
            ],
        ),
        (
            ProtarrowConfig(enum_type=pa.dictionary(pa.int32(), pa.string())),
            [
                ["UNKNOWN_EXAMPLE_ENUM", "EXAMPLE_ENUM_1", "UNKNOWN_EXAMPLE_ENUM"],
                [],
                [],
            ],
        ),
        (
            ProtarrowConfig(enum_type=pa.binary()),
            [
                [b"UNKNOWN_EXAMPLE_ENUM", b"EXAMPLE_ENUM_1", b"UNKNOWN_EXAMPLE_ENUM"],
                [],
                [],
            ],
        ),
    ],
)
def test_repeated_enum_values_as_string(config: ProtarrowConfig, expected: list):
    records = [
        ExampleMessage(example_enum_values=[0, 1, 0]),
        ExampleMessage(example_enum_values=[]),
        ExampleMessage(),
    ]

    array = _repeated_proto_to_array(
        NestedIterable(records, lambda x: x.example_enum_values),
        ExampleMessage.DESCRIPTOR.fields_by_name["example_enum_values"],
        config,
    )
    assert array.to_pylist() == expected


def test_nested_list_can_be_null():
    messages = [
        NestedExampleMessage(),
        NestedExampleMessage(example_message=ExampleMessage()),
    ]
    record_batch = messages_to_record_batch(messages, NestedExampleMessage)
    field_index = record_batch["example_message"].type.get_field_index("double_values")
    double_values = record_batch["example_message"].field(field_index)
    assert double_values.is_valid().to_pylist() == [False, True]


def test_unit_for_time_of_day():
    for type in [
        pa.time64("ns"),
        pa.time64("us"),
        pa.time32("ms"),
        pa.time32("s"),
    ]:
        assert (
            message_type_to_schema(
                ExampleMessage, ProtarrowConfig(time_of_day_type=type)
            )
            .field("time_of_day_value")
            .type
            == type
        )


def test_check_init_sorted():
    assert protarrow.__all__ == sorted(protarrow.__all__)


def test_truncate_nanos():

    assert truncate_nanos(
        Timestamp(seconds=10, nanos=123456789),
        "s",
        "us",
    ) == Timestamp(seconds=10)

    assert truncate_nanos(
        Timestamp(seconds=10, nanos=123456789), "ms", "us"
    ) == Timestamp(seconds=10, nanos=123000000)

    assert truncate_nanos(
        Timestamp(seconds=10, nanos=123456789), "us", "us"
    ) == Timestamp(seconds=10, nanos=123456000)

    assert truncate_nanos(
        Timestamp(seconds=10, nanos=123456789), "ns", "us"
    ) == Timestamp(seconds=10, nanos=123456789)

    assert truncate_nanos(
        TimeOfDay(seconds=10, nanos=123456789), "ns", "us"
    ) == TimeOfDay(seconds=10, nanos=123456000)


def test_truncate_nested():
    assert truncate_nanos(
        ExampleMessage(
            timestamp_value=Timestamp(seconds=10, nanos=123_456_789),
            timestamp_string_map={"foo": Timestamp(seconds=10, nanos=123_456_789)},
            time_of_day_value=TimeOfDay(hours=1, nanos=123_456_789),
        ),
        "us",
        "ms",
    ) == ExampleMessage(
        timestamp_value=Timestamp(seconds=10, nanos=123_456_000),
        timestamp_string_map={"foo": Timestamp(seconds=10, nanos=123_456_000)},
        time_of_day_value=TimeOfDay(hours=1, nanos=123_000_000),
    )


@pytest.mark.parametrize(
    "enum_type", [pa.binary(), pa.dictionary(pa.int32(), pa.binary())]
)
def test_binary_enums(enum_type: pa.DataType):
    message = ExampleMessage(example_enum_value=2)
    table = messages_to_table(
        [message], ExampleMessage, ProtarrowConfig(enum_type=enum_type)
    )
    assert table["example_enum_value"].to_pylist() == [b"EXAMPLE_ENUM_2"]
    assert table["example_enum_value"].type == enum_type

    message_back = table_to_messages(table, ExampleMessage)[0]
    assert message == message_back


@pytest.mark.parametrize(
    "enum_type", [pa.string(), pa.dictionary(pa.int32(), pa.string())]
)
def test_string_enums(enum_type: pa.DataType):
    message = ExampleMessage(example_enum_value=2)
    table = messages_to_table(
        [message], ExampleMessage, ProtarrowConfig(enum_type=enum_type)
    )
    assert table["example_enum_value"].to_pylist() == ["EXAMPLE_ENUM_2"]
    assert table["example_enum_value"].type == enum_type

    message_back = table_to_messages(table, ExampleMessage)[0]
    assert message == message_back


@pytest.mark.parametrize(
    ["enum_type", "expected"],
    [
        (pa.int32(), 0),
        (pa.binary(), b"UNKNOWN_EXAMPLE_ENUM"),
        (pa.dictionary(pa.int32(), pa.binary()), b"UNKNOWN_EXAMPLE_ENUM"),
        (pa.string(), "UNKNOWN_EXAMPLE_ENUM"),
        (pa.dictionary(pa.int32(), pa.string()), "UNKNOWN_EXAMPLE_ENUM"),
    ],
)
def test_get_arrow_default_value(enum_type: pa.DataType, expected: Any):

    assert (
        get_arrow_default_value(
            ExampleMessage.DESCRIPTOR.fields_by_name["example_enum_value"],
            ProtarrowConfig(enum_type=enum_type),
        )
        == expected
    )
    assert (
        get_arrow_default_value(
            ExampleMessage.DESCRIPTOR.fields_by_name["example_enum_values"],
            ProtarrowConfig(enum_type=enum_type),
        )
        == expected
    )


def _check_messages_same(actual: Iterable[Message], expected: Iterable[Message]):
    for left, right in zip(actual, expected):
        assert left == right
    assert actual == expected


def test_nested_field_values_not_null_when_message_missing():
    messages = [NestedExampleMessage()]
    record_batch = messages_to_record_batch(messages, NestedExampleMessage)
    assert record_batch["example_message"].null_count == 1
    assert record_batch["example_message"].to_pylist() == [None]
    assert record_batch["example_message"].field(0).null_count == 0
    assert record_batch["example_message"].field(0).to_pylist() == [0.0]
    assert record_batch["example_message"].type.field(0).name == "double_value"


def test_nested_enums():
    messages = [NestedExampleMessage()]
    record_batch = messages_to_record_batch(
        messages, NestedExampleMessage, ProtarrowConfig(enum_type=pa.binary())
    )
    assert record_batch["example_message"].field(
        record_batch["example_message"].type.get_field_index("example_enum_value")
    ).to_pylist() == [b"UNKNOWN_EXAMPLE_ENUM"]


def test_create_enum_converter_wrong_type():
    with pytest.raises(TypeError, match=r"double"):
        create_enum_converter(
            ExampleMessage.DESCRIPTOR.fields_by_name["example_enum_value"].enum_type,
            pa.float64(),
        )


@pytest.mark.parametrize("message_type", MESSAGES)
@pytest.mark.parametrize("config", CONFIGS)
def test_cast_empty(message_type: Type[Message], config: ProtarrowConfig):
    table = pa.table({"nulls": pa.nulls(10, pa.null())})
    casted_table = cast_table(table, message_type, config)
    assert len(table) == len(casted_table)
    assert casted_table.schema == message_type_to_schema(message_type, config)


@pytest.mark.parametrize("message_type", MESSAGES)
@pytest.mark.parametrize("config", CONFIGS)
def test_cast_same(message_type: Type[Message], config: ProtarrowConfig):
    source_messages = [
        truncate_nanos(m, config.timestamp_type.unit, config.time_of_day_type.unit)
        for m in generate_messages(message_type, 10)
    ]
    table = messages_to_table(source_messages, message_type, config)
    casted_table = cast_table(table, message_type, config)
    assert table == casted_table


def test_is_custom_field():
    assert not is_custom_field(
        ExampleMessage.DESCRIPTOR.fields_by_name["wrapped_double_value"]
    )
    assert not is_custom_field(
        ExampleMessage.DESCRIPTOR.fields_by_name["wrapped_double_values"]
    )


@pytest.mark.parametrize("message_type", MESSAGES)
@pytest.mark.parametrize("enum_value_type", [pa.string(), pa.binary()])
def test_can_cast_enum_to_dictionary_and_back(
    message_type: Type[Message], enum_value_type: pa.DataType
):
    plain_config = ProtarrowConfig(enum_type=pa.string())
    dict_config = ProtarrowConfig(enum_type=pa.dictionary(pa.int32(), pa.string()))

    source_messages = generate_messages(message_type, 10)

    plain_table = messages_to_table(source_messages, message_type, plain_config)
    dict_table = messages_to_table(source_messages, message_type, dict_config)
    plain_table_as_dict = cast_table(plain_table, message_type, dict_config)

    assert dict_table == plain_table_as_dict
    plain_table_back = cast_table(plain_table_as_dict, message_type, plain_config)
    assert plain_table_back == plain_table
