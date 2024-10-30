import pathlib
from typing import Any, Iterable, List, Type

import pyarrow as pa
import pytest
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.duration_pb2 import Duration
from google.protobuf.empty_pb2 import Empty
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
from protarrow.cast_to_proto import (
    _cast_array,
    cast_table,
    get_arrow_default_value,
    get_casted_array,
    maybe_copy_offsets,
)
from protarrow.common import M, ProtarrowConfig, offset_values_array
from protarrow.message_extractor import MessageExtractor
from protarrow.proto_to_arrow import (
    NestedIterable,
    _messages_to_array,
    _repeated_proto_to_array,
    field_descriptor_to_field,
    message_type_to_schema,
    message_type_to_struct_type,
    messages_to_record_batch,
    messages_to_table,
)
from protarrow_protos.bench_pb2 import (
    ExampleMessage,
    NestedExampleMessage,
    SuperNestedExampleMessage,
)
from protarrow_protos.example_pb2 import EmptyMessage, NestedEmptyMessage
from tests.random_generator import generate_messages, truncate_messages, truncate_nanos

TEST_MESSAGE_COUNT = 5
MESSAGES = [ExampleMessage, NestedExampleMessage, SuperNestedExampleMessage]
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
    ProtarrowConfig(time_of_day_type=pa.time32("ms")),
    ProtarrowConfig(time_of_day_type=pa.time32("s")),
    ProtarrowConfig(duration_type=pa.duration("s")),
    ProtarrowConfig(duration_type=pa.duration("ms")),
    ProtarrowConfig(duration_type=pa.duration("us")),
    ProtarrowConfig(duration_type=pa.duration("ns")),
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
    source_messages = generate_messages(message_type, 10)
    table = messages_to_table(source_messages, message_type, config)
    messages_back = table_to_messages(table, message_type)
    truncated_messages = truncate_messages(source_messages, config)
    _check_messages_same(truncated_messages, messages_back)


@pytest.mark.parametrize("message_type", MESSAGES)
@pytest.mark.parametrize("config", CONFIGS)
@pytest.mark.parametrize("index", [0, 1, 3])
def test_with_random_not_aligned(
    message_type: Type[Message], config: ProtarrowConfig, index: int
):
    source_messages = generate_messages(message_type, 3)
    table = messages_to_table(source_messages, message_type, config)
    messages_back = table_to_messages(table[index:], message_type)
    truncated_messages = truncate_messages(source_messages[index:], config)
    _check_messages_same(truncated_messages, messages_back)


@pytest.mark.parametrize("message_type", MESSAGES)
@pytest.mark.parametrize("config", CONFIGS)
def test_with_sample_data(message_type: Type[Message], config: ProtarrowConfig):
    source_file = (
        pathlib.Path(__file__).parent / "data" / f"{message_type.DESCRIPTOR.name}.jsonl"
    )
    source_messages = read_proto_jsonl(source_file, message_type)

    table = messages_to_table(source_messages, message_type, config)
    messages_back = table_to_messages(table, message_type)
    truncated_messages = truncate_messages(source_messages, config)
    _check_messages_same(truncated_messages, messages_back)


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


def test_messages_to_array_empty():
    assert _messages_to_array(
        [Empty(), Empty()], Empty.DESCRIPTOR, None, ProtarrowConfig()
    ) == pa.StructArray.from_arrays([], names=[], mask=pa.array([False, False]))


def test_empty_values():
    records = [
        ExampleMessage(empty_values=[Empty(), Empty(), Empty()]),
        ExampleMessage(empty_values=[]),
        ExampleMessage(),
    ]

    array = _repeated_proto_to_array(
        NestedIterable(records, lambda x: x.empty_values),
        ExampleMessage.DESCRIPTOR.fields_by_name["empty_values"],
        ProtarrowConfig(),
    )
    assert array.to_pylist() == [[{}, {}, {}], [], []]


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
        Timestamp(seconds=10, nanos=123456789), "s", "us", "s"
    ) == Timestamp(seconds=10)

    assert truncate_nanos(
        Timestamp(seconds=10, nanos=123456789), "ms", "us", "s"
    ) == Timestamp(seconds=10, nanos=123000000)

    assert truncate_nanos(
        Timestamp(seconds=10, nanos=123456789), "us", "us", "s"
    ) == Timestamp(seconds=10, nanos=123456000)

    assert truncate_nanos(
        Timestamp(seconds=10, nanos=123456789), "ns", "us", "s"
    ) == Timestamp(seconds=10, nanos=123456789)

    assert truncate_nanos(
        TimeOfDay(seconds=10, nanos=123456789), "ns", "us", "s"
    ) == TimeOfDay(seconds=10, nanos=123456000)

    assert truncate_nanos(
        Duration(seconds=10, nanos=123456789), "ns", "us", "us"
    ) == Duration(seconds=10, nanos=123456000)


def test_truncate_nested():
    assert truncate_nanos(
        ExampleMessage(
            timestamp_value=Timestamp(seconds=10, nanos=123_456_789),
            timestamp_string_map={"foo": Timestamp(seconds=10, nanos=123_456_789)},
            time_of_day_value=TimeOfDay(hours=1, nanos=123_456_789),
        ),
        "us",
        "ms",
        "ns",
    ) == ExampleMessage(
        timestamp_value=Timestamp(seconds=10, nanos=123_456_000),
        timestamp_string_map={"foo": Timestamp(seconds=10, nanos=123_456_000)},
        time_of_day_value=TimeOfDay(hours=1, nanos=123_000_000),
    )


def test_truncate_nested_nested():
    assert truncate_nanos(
        NestedExampleMessage(
            example_message=ExampleMessage(
                timestamp_value=Timestamp(seconds=10, nanos=123_456_789),
                timestamp_string_map={"foo": Timestamp(seconds=10, nanos=123_456_789)},
                time_of_day_value=TimeOfDay(hours=1, nanos=123_456_789),
            ),
        ),
        "us",
        "ms",
        "ns",
    ) == NestedExampleMessage(
        example_message=ExampleMessage(
            timestamp_value=Timestamp(seconds=10, nanos=123_456_000),
            timestamp_string_map={"foo": Timestamp(seconds=10, nanos=123_456_000)},
            time_of_day_value=TimeOfDay(hours=1, nanos=123_000_000),
        )
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
    table = pa.table({"nulls": pa.nulls(TEST_MESSAGE_COUNT, pa.null())})
    casted_table = cast_table(table, message_type, config)
    assert len(table) == len(casted_table)
    assert casted_table.schema == message_type_to_schema(message_type, config)


@pytest.mark.parametrize("message_type", MESSAGES)
@pytest.mark.parametrize("config", CONFIGS)
def test_cast_same(message_type: Type[Message], config: ProtarrowConfig):
    source_messages = generate_messages(message_type, TEST_MESSAGE_COUNT)
    table = messages_to_table(source_messages, message_type, config)
    casted_table = cast_table(table, message_type, config)
    assert table == casted_table


@pytest.mark.parametrize("message_type", MESSAGES)
@pytest.mark.parametrize("config", CONFIGS)
def test_cast_same_view(message_type: Type[Message], config: ProtarrowConfig):
    source_messages = generate_messages(message_type, TEST_MESSAGE_COUNT)
    table = messages_to_table(source_messages, message_type, config)

    assert 2 + 1 < TEST_MESSAGE_COUNT, "View too small"
    view = table[2:]
    casted_view = cast_table(view, message_type, config)
    assert casted_view == view


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

    source_messages = generate_messages(message_type, TEST_MESSAGE_COUNT)

    plain_table = messages_to_table(source_messages, message_type, plain_config)
    dict_table = messages_to_table(source_messages, message_type, dict_config)
    plain_table_as_dict = cast_table(plain_table, message_type, dict_config)

    assert dict_table == plain_table_as_dict
    plain_table_back = cast_table(plain_table_as_dict, message_type, plain_config)
    assert plain_table_back == plain_table


@pytest.mark.parametrize("message_type", MESSAGES)
@pytest.mark.parametrize("config", CONFIGS)
def test_extractor(message_type: Type[Message], config: ProtarrowConfig):
    source_messages = generate_messages(message_type, TEST_MESSAGE_COUNT)

    table = messages_to_table(source_messages, message_type, config)
    message_extractor = MessageExtractor(table.schema, message_type)
    messages_back = [
        message_extractor.read_table_row(table, row) for row in range(len(table))
    ]
    truncated_messages = truncate_messages(source_messages, config)
    _check_messages_same(truncated_messages, messages_back)


@pytest.mark.parametrize("message_type", MESSAGES)
@pytest.mark.parametrize("config", CONFIGS)
def test_extractor_null_values(message_type: Type[Message], config: ProtarrowConfig):
    table = protarrow.cast_table(
        pa.table({"nulls": pa.nulls(10)}), message_type, config
    )

    message_extractor = MessageExtractor(table.schema, message_type)
    messages = [
        message_extractor.read_table_row(table, row) for row in range(len(table))
    ]
    assert messages == [message_type()] * len(table)


def test_empty():
    source_messages = [
        ExampleMessage(empty_value=Empty()),
        ExampleMessage(),
    ]

    table = messages_to_table(source_messages, ExampleMessage, ProtarrowConfig())
    messages_back = table_to_messages(table, ExampleMessage)
    _check_messages_same(source_messages, messages_back)


@pytest.mark.parametrize("config", CONFIGS[:-1])
def test_only_messages_default_to_null_on_missing_array(config):
    """
    Missing arrays in cast should have null only for nested (non-repeated) message.

    TODO: Add the last config when MapScaler.as_py is fixed
     see https://github.com/apache/arrow/issues/36809
    """
    for field_descriptor in NestedExampleMessage.DESCRIPTOR.fields:
        expected = (
            None
            if field_descriptor.type == FieldDescriptor.TYPE_MESSAGE
            and field_descriptor.label != FieldDescriptor.LABEL_REPEATED
            else []
        )
        assert get_casted_array(field_descriptor, None, 1, config)[0].to_pylist() == [
            expected
        ]


@pytest.mark.parametrize("config", CONFIGS[:-1])
def test_only_messages_stay_to_null_on_casted_array(config):
    """
    Arrays with null in cast should have null only for nested (non-repeated) message.

    TODO: Add the last config when MapScaler.as_py is fixed
     see https://github.com/apache/arrow/issues/36809
    """
    for field_descriptor in NestedExampleMessage.DESCRIPTOR.fields:
        expected = (
            None
            if field_descriptor.type == FieldDescriptor.TYPE_MESSAGE
            and field_descriptor.label != FieldDescriptor.LABEL_REPEATED
            else []
        )
        arrow_field = field_descriptor_to_field(field_descriptor, config)
        assert get_casted_array(
            field_descriptor,
            pa.array([None], arrow_field.type),
            1,
            config=config,
        )[0].to_pylist() == [expected]


def test_repeated_primitives_array_slice():
    source_messages = [
        ExampleMessage(int32_values=[1, 2, 3]),
        ExampleMessage(int32_values=[4, 5, 6]),
    ]
    table = messages_to_table(source_messages, ExampleMessage, ProtarrowConfig())
    messages_back = table_to_messages(table[1:], ExampleMessage)
    _check_messages_same(source_messages[1:], messages_back)


def test_repeated_message_array_slice():
    source_messages = [
        NestedExampleMessage(
            repeated_example_message=[
                ExampleMessage(int32_value=1),
                ExampleMessage(int32_value=2),
                ExampleMessage(int32_value=3),
            ]
        ),
        NestedExampleMessage(
            repeated_example_message=[
                ExampleMessage(int32_value=4),
                ExampleMessage(int32_value=5),
                ExampleMessage(int32_value=6),
            ]
        ),
    ]
    table = messages_to_table(source_messages, NestedExampleMessage, ProtarrowConfig())
    messages_back = table_to_messages(table[1:], NestedExampleMessage)
    _check_messages_same(source_messages[1:], messages_back)


def test_map_message_array_slice():
    source_messages = [
        NestedExampleMessage(
            example_message_int32_map={
                1: ExampleMessage(int32_value=1),
                2: ExampleMessage(int32_value=2),
                3: ExampleMessage(int32_value=3),
            }
        ),
        NestedExampleMessage(
            example_message_int32_map={
                4: ExampleMessage(int32_value=4),
                5: ExampleMessage(int32_value=5),
                6: ExampleMessage(int32_value=6),
            }
        ),
    ]
    table = messages_to_table(source_messages, NestedExampleMessage, ProtarrowConfig())
    messages_back = table_to_messages(table[1:], NestedExampleMessage)
    _check_messages_same(source_messages[1:], messages_back)


def test_primitive_map_message_array_slice():
    source_messages = [
        NestedExampleMessage(
            example_message_int32_map={
                1: ExampleMessage(double_int32_map={1: 1.1}),
                2: ExampleMessage(double_int32_map={2: 2.2}),
                3: ExampleMessage(double_int32_map={3: 3.3}),
            }
        ),
        NestedExampleMessage(
            example_message_int32_map={
                4: ExampleMessage(double_int32_map={4: 4.4}),
                5: ExampleMessage(double_int32_map={5: 5.5}),
                6: ExampleMessage(double_int32_map={6: 6.6}),
            }
        ),
    ]
    table = messages_to_table(source_messages, NestedExampleMessage, ProtarrowConfig())
    messages_back = table_to_messages(table[1:], NestedExampleMessage)
    _check_messages_same(source_messages[1:], messages_back)


def test_empty_nested_message():
    source_messages = [
        NestedEmptyMessage(empty_message=EmptyMessage()),
        NestedEmptyMessage(empty_message=EmptyMessage()),
    ]
    table = messages_to_table(source_messages, NestedEmptyMessage, ProtarrowConfig())
    messages_back = table_to_messages(table[1:], NestedEmptyMessage)
    _check_messages_same(source_messages[1:], messages_back)


def test_empty_nested_message_has_has_not():
    source_messages = [
        NestedEmptyMessage(empty_message=EmptyMessage(empty_value=Empty())),
        NestedEmptyMessage(empty_message=EmptyMessage()),
        NestedEmptyMessage(),
    ]
    table = messages_to_table(source_messages, NestedEmptyMessage, ProtarrowConfig())
    messages_back = table_to_messages(table, NestedEmptyMessage)
    _check_messages_same(source_messages, messages_back)
    assert messages_back[0].empty_message.HasField("empty_value")
    assert not messages_back[1].empty_message.HasField("empty_value")
    assert not messages_back[1].empty_message.HasField("empty_value")


def test_empty_repeated_message():
    source_messages = [
        NestedEmptyMessage(
            repeated_empty_message=[
                EmptyMessage(),
                EmptyMessage(),
                EmptyMessage(),
            ]
        ),
        NestedEmptyMessage(
            repeated_empty_message=[
                EmptyMessage(),
                EmptyMessage(),
                EmptyMessage(),
            ]
        ),
    ]
    table = messages_to_table(source_messages, NestedEmptyMessage, ProtarrowConfig())
    messages_back = table_to_messages(table[1:], NestedEmptyMessage)
    _check_messages_same(source_messages[1:], messages_back)


def test_offset_values_array():
    array = pa.array([[1], [1, 2], [1, 2, 3]])
    slice_0 = array[0:]
    assert offset_values_array(slice_0, slice_0.values).to_pylist() == [
        1,
        1,
        2,
        1,
        2,
        3,
    ]

    slice_1 = array[1:]
    assert offset_values_array(slice_1, slice_1.values).to_pylist() == [1, 2, 1, 2, 3]

    slice_2 = array[2:]
    assert offset_values_array(slice_2, slice_2.values).to_pylist() == [1, 2, 3]

    slice_3 = array[3:]
    assert offset_values_array(slice_3, slice_3.values).to_pylist() == []

    slice_m1 = array[-1:]
    assert offset_values_array(slice_m1, slice_m1.values).to_pylist() == [1, 2, 3]


def test_cast_map_offset():
    config = ProtarrowConfig()
    field = ExampleMessage.DESCRIPTOR.fields_by_name["int32_string_map"]
    values = [
        [("foo", 123), ("bar", 123)],
        [("foo", 456), ("bar", 456)],
    ]
    from_array = pa.array(values, pa.map_(pa.string(), pa.int64()))
    to_array = pa.array(
        values,
        pa.map_(
            pa.string(),
            pa.field(
                config.map_value_name,
                pa.int32(),
                nullable=config.map_value_nullable,
            ),
        ),
    )
    assert _cast_array(from_array, field, ProtarrowConfig()) == to_array

    assert _cast_array(from_array[1:], field, ProtarrowConfig()) == to_array[1:]


def test_cast_list_offset():
    config = ProtarrowConfig()
    field = ExampleMessage.DESCRIPTOR.fields_by_name["int32_values"]
    values = [
        [1, 2, 3],
        [4, 5, 6],
    ]
    from_array = pa.array(values, pa.list_(pa.int32()))
    to_array = pa.array(
        values,
        pa.list_(
            pa.field(
                config.list_value_name,
                pa.int32(),
                nullable=config.list_value_nullable,
            ),
        ),
    )

    assert _cast_array(from_array, field, ProtarrowConfig()) == to_array
    assert _cast_array(from_array[1:], field, ProtarrowConfig()) == to_array[1:]


def test_maybe_copy_offsets():
    array = pa.array([1, 2, 3], pa.int32())
    assert maybe_copy_offsets(array) is array  # no copy
    view = array[1:]
    copy = maybe_copy_offsets(array[1:])
    assert copy == view
    assert copy is not view  # copy
    assert copy.offset == 0
    # Sadly pa.array is highly optimized
    assert pa.array(array) is array


def test_map_cast():
    array = pa.array(
        [
            [(123, 123), (456, 456)],
            [(7, 7), (8, 8)],
        ],
        pa.map_(pa.int32(), pa.int64()),
    )
    table = pa.table({"int32_int32_map": array})

    assert (
        cast_table(table, ExampleMessage, ProtarrowConfig())[
            "int32_int32_map"
        ].to_pylist()
        == array.to_pylist()
    )
    # Again with a view:
    assert (
        cast_table(table[1:], ExampleMessage, ProtarrowConfig())[
            "int32_int32_map"
        ].to_pylist()
        == array[1:].to_pylist()
    )


@pytest.mark.parametrize("config", CONFIGS)
def test_duration(config):
    messages = [ExampleMessage(duration_value=Duration(seconds=10, nanos=123456789))]
    table = protarrow.messages_to_record_batch(messages, ExampleMessage, config)
    assert isinstance(table, pa.RecordBatch)
    messages_back = protarrow.record_batch_to_messages(table, ExampleMessage)
    expected = truncate_messages(messages, config)
    assert messages_back == expected


def test_duration_specific():
    messages = [ExampleMessage(duration_value=Duration(seconds=10, nanos=123456789))]
    config = protarrow.ProtarrowConfig(duration_type=pa.duration("us"))
    expected = truncate_messages(messages, config)
    table = protarrow.messages_to_record_batch(messages, ExampleMessage, config)
    assert table.schema.field("duration_value").type == pa.duration("us")
    assert isinstance(table, pa.RecordBatch)
    messages_back = protarrow.record_batch_to_messages(table, ExampleMessage)
    assert messages_back == expected
