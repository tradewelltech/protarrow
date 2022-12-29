import collections.abc
import dataclasses
import datetime
import operator
from typing import (
    Any,
    Callable,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)

import pyarrow as pa
import pyarrow.compute as pc
from google.protobuf.descriptor import Descriptor, EnumDescriptor, FieldDescriptor
from google.protobuf.descriptor_pb2 import FieldDescriptorProto
from google.protobuf.internal.containers import MessageMap, RepeatedScalarFieldContainer
from google.protobuf.message import Message
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.wrappers_pb2 import (
    BoolValue,
    BytesValue,
    DoubleValue,
    FloatValue,
    Int32Value,
    Int64Value,
    StringValue,
    UInt32Value,
    UInt64Value,
)
from google.type.date_pb2 import Date
from google.type.timeofday_pb2 import TimeOfDay

from protarrow.common import M, ProtarrowConfig, is_binary_enum, is_string_enum

_PROTO_DESCRIPTOR_TO_PYARROW = {
    Date.DESCRIPTOR: pa.date32(),
    BoolValue.DESCRIPTOR: pa.bool_(),
    BytesValue.DESCRIPTOR: pa.binary(),
    DoubleValue.DESCRIPTOR: pa.float64(),
    FloatValue.DESCRIPTOR: pa.float32(),
    Int32Value.DESCRIPTOR: pa.int32(),
    Int64Value.DESCRIPTOR: pa.int64(),
    StringValue.DESCRIPTOR: pa.string(),
    UInt32Value.DESCRIPTOR: pa.uint32(),
    UInt64Value.DESCRIPTOR: pa.uint64(),
}

_PROTO_PRIMITIVE_TYPE_TO_PYARROW = {
    FieldDescriptorProto.TYPE_DOUBLE: pa.float64(),
    FieldDescriptorProto.TYPE_FLOAT: pa.float32(),
    FieldDescriptorProto.TYPE_INT64: pa.int64(),
    FieldDescriptorProto.TYPE_UINT64: pa.uint64(),
    FieldDescriptorProto.TYPE_INT32: pa.int32(),
    FieldDescriptorProto.TYPE_FIXED64: pa.uint64(),
    FieldDescriptorProto.TYPE_FIXED32: pa.uint32(),
    FieldDescriptorProto.TYPE_BOOL: pa.bool_(),
    FieldDescriptorProto.TYPE_STRING: pa.string(),
    FieldDescriptorProto.TYPE_BYTES: pa.binary(),
    FieldDescriptorProto.TYPE_UINT32: pa.uint32(),
    FieldDescriptorProto.TYPE_SFIXED32: pa.int32(),
    FieldDescriptorProto.TYPE_SFIXED64: pa.int64(),
    FieldDescriptorProto.TYPE_SINT32: pa.int32(),
    FieldDescriptorProto.TYPE_SINT64: pa.int64(),
}


def _time_of_day_to_nanos(time_of_day: TimeOfDay) -> int:
    return (
        (time_of_day.hours * 60 + time_of_day.minutes) * 60 + time_of_day.seconds
    ) * 1_000_000_000 + time_of_day.nanos


def _time_of_day_to_micros(time_of_day: TimeOfDay) -> int:
    return (
        (time_of_day.hours * 60 + time_of_day.minutes) * 60 + time_of_day.seconds
    ) * 1_000_000 + time_of_day.nanos // 1_000


def _time_of_day_to_millis(time_of_day: TimeOfDay) -> int:
    return (
        (time_of_day.hours * 60 + time_of_day.minutes) * 60 + time_of_day.seconds
    ) * 1_000 + time_of_day.nanos // 1_000_000


def _time_of_day_to_seconds(time_of_day: TimeOfDay) -> int:
    return (time_of_day.hours * 60 + time_of_day.minutes) * 60 + time_of_day.seconds


def _proto_date_to_py_date(proto_date: Date) -> datetime.date:
    # TODO: fix when
    x: datetime.date.min
    if proto_date.year == 0:
        return datetime.date.min
    else:
        return datetime.date(proto_date.year, proto_date.month, proto_date.day)


_PROTO_DESCRIPTOR_TO_ARROW_CONVERTER = {
    Date.DESCRIPTOR: _proto_date_to_py_date,
    TimeOfDay.DESCRIPTOR: _time_of_day_to_nanos,
    BoolValue.DESCRIPTOR: operator.attrgetter("value"),
    BytesValue.DESCRIPTOR: operator.attrgetter("value"),
    DoubleValue.DESCRIPTOR: operator.attrgetter("value"),
    FloatValue.DESCRIPTOR: operator.attrgetter("value"),
    Int32Value.DESCRIPTOR: operator.attrgetter("value"),
    Int64Value.DESCRIPTOR: operator.attrgetter("value"),
    StringValue.DESCRIPTOR: operator.attrgetter("value"),
    UInt32Value.DESCRIPTOR: operator.attrgetter("value"),
    UInt64Value.DESCRIPTOR: operator.attrgetter("value"),
}

_TIMESTAMP_CONVERTERS = {
    "s": Timestamp.ToSeconds,
    "ms": Timestamp.ToMilliseconds,
    "us": Timestamp.ToMicroseconds,
    "ns": Timestamp.ToNanoseconds,
}

_TIME_OF_DAY_CONVERTERS = {
    "s": _time_of_day_to_seconds,
    "ms": _time_of_day_to_millis,
    "us": _time_of_day_to_micros,
    "ns": _time_of_day_to_nanos,
}


@dataclasses.dataclass(frozen=True)
class FlattenedIterable(collections.abc.Iterable):
    parents: Iterable[Iterable[Optional[Any]]]

    def __iter__(self) -> Iterator[Any]:
        for parent in self.parents:
            if parent is not None:
                for child in parent:
                    yield child

    def __len__(self) -> int:
        return sum(len(i) for i in self.parents if i)


@dataclasses.dataclass(frozen=True)
class NestedIterable(collections.abc.Iterable):
    parents: Iterable[Optional[Message]]
    getter: Callable[[Message], Any]

    def __iter__(self) -> Iterator[Optional[Any]]:
        for parent in self.parents:
            if parent is not None:
                yield self.getter(parent)
            else:
                yield None

    def __len__(self) -> int:
        return len(self.parents)


@dataclasses.dataclass(frozen=True)
class NestedMessageGetter:
    name: str

    def __call__(self, message: Message) -> Optional[Message]:
        if message.HasField(self.name):
            return getattr(message, self.name)
        else:
            return None


@dataclasses.dataclass(frozen=True)
class MapKeyIterable(collections.abc.Iterable):
    scalar_map: Iterable[Optional[MessageMap]]

    def __iter__(self) -> Iterator[Any]:
        for scalar_map in self.scalar_map:
            if scalar_map is not None:
                for key in scalar_map.keys():
                    yield key


@dataclasses.dataclass(frozen=True)
class MapValueIterable(collections.abc.Iterable):
    scalar_map: Iterable[Optional[MessageMap]]

    def __iter__(self) -> Iterator[Any]:
        for scalar_map in self.scalar_map:
            if scalar_map is not None:
                for value in scalar_map.values():
                    yield value

    def __len__(self) -> int:
        return sum(len(i) for i in self.scalar_map if i)


def is_map(field_descriptor: FieldDescriptor) -> bool:
    return (
        field_descriptor.type == FieldDescriptor.TYPE_MESSAGE
        and field_descriptor.label == FieldDescriptor.LABEL_REPEATED
        and field_descriptor.message_type.GetOptions().map_entry
    )


def get_map_descriptors(
    field_descriptor: FieldDescriptor,
) -> Tuple[FieldDescriptor, FieldDescriptor]:
    return (
        field_descriptor.message_type.fields_by_name["key"],
        field_descriptor.message_type.fields_by_name["value"],
    )


def get_enum_converter(
    data_type: pa.DataType, enum_descriptor: EnumDescriptor
) -> Callable[[int], Any]:
    if pa.types.is_integer(data_type):
        return lambda x: x

    elif is_binary_enum(data_type):
        values_by_number = {
            k: bytes(v.name, encoding="UTF-8")
            for k, v in enum_descriptor.values_by_number.items()
        }

        def converter(x: int) -> bytes:
            return values_by_number[x]

        return converter
    elif is_string_enum(data_type):
        values_by_number = {
            k: v.name for k, v in enum_descriptor.values_by_number.items()
        }

        def converter(x: int) -> bytes:
            return values_by_number[x]

        return converter
    else:
        raise TypeError(data_type)


def field_descriptor_to_field(
    field_descriptor: FieldDescriptor,
    config: ProtarrowConfig,
) -> pa.Field:
    if is_map(field_descriptor):
        key_field, value_field = get_map_descriptors(field_descriptor)
        key_type = field_descriptor_to_data_type(key_field, config)
        value_type = field_descriptor_to_data_type(value_field, config)
        return pa.field(
            field_descriptor.name,
            pa.map_(
                key_type,
                pa.field(config.map_value_name, value_type, config.map_value_nullable),
            ),
            nullable=config.map_nullable,
        )
    elif field_descriptor.label == FieldDescriptor.LABEL_REPEATED:
        return pa.field(
            field_descriptor.name,
            pa.list_(
                pa.field(
                    config.list_value_name,
                    field_descriptor_to_data_type(field_descriptor, config),
                    nullable=config.list_value_nullable,
                ),
            ),
            nullable=config.list_nullable,
        )
    else:
        return pa.field(
            field_descriptor.name,
            field_descriptor_to_data_type(field_descriptor, config),
            nullable=field_descriptor.type == FieldDescriptor.TYPE_MESSAGE,
        )


def field_descriptor_to_data_type(
    field_descriptor: FieldDescriptor,
    config: ProtarrowConfig,
) -> pa.DataType:
    if field_descriptor.message_type == Timestamp.DESCRIPTOR:
        return config.timestamp_type
    elif field_descriptor.message_type == TimeOfDay.DESCRIPTOR:
        return config.time_of_day_type
    elif field_descriptor.type == FieldDescriptorProto.TYPE_MESSAGE:
        try:
            return _PROTO_DESCRIPTOR_TO_PYARROW[field_descriptor.message_type]
        except KeyError:
            return pa.struct(
                [
                    field_descriptor_to_field(child_field, config)
                    for child_field in field_descriptor.message_type.fields
                ]
            )
    elif field_descriptor.type == FieldDescriptorProto.TYPE_ENUM:
        return config.enum_type
    elif field_descriptor.type in _PROTO_PRIMITIVE_TYPE_TO_PYARROW:
        return _PROTO_PRIMITIVE_TYPE_TO_PYARROW.get(field_descriptor.type)
    else:
        raise TypeError(
            f"Unsupported field type "
            f"{FieldDescriptorProto.Type.Name(field_descriptor.type)} "
            f"for {field_descriptor.name}"
        )


def _get_converter(
    field_descriptor: FieldDescriptor,
    config: ProtarrowConfig,
) -> Optional[Callable[[Any], Any]]:
    if field_descriptor.message_type == Timestamp.DESCRIPTOR:
        return _TIMESTAMP_CONVERTERS[config.timestamp_type.unit]
    elif field_descriptor.message_type == TimeOfDay.DESCRIPTOR:
        return _TIME_OF_DAY_CONVERTERS[config.time_of_day_type.unit]
    elif field_descriptor.type == FieldDescriptorProto.TYPE_MESSAGE:
        # This may return None, in which case you need to convert
        # each underlying field to array and put them back together
        # in a StructArray
        return _PROTO_DESCRIPTOR_TO_ARROW_CONVERTER.get(field_descriptor.message_type)
    elif field_descriptor.type == FieldDescriptorProto.TYPE_ENUM:
        return get_enum_converter(config.enum_type, field_descriptor.enum_type)
    elif field_descriptor.type in _PROTO_PRIMITIVE_TYPE_TO_PYARROW:
        return lambda x: x
    else:
        raise TypeError(
            f"Unsupported field type "
            f"{FieldDescriptorProto.Type.Name(field_descriptor.type)} "
            f"for {field_descriptor.name}"
        )


def _proto_field_to_array(
    proto_field_values: Iterable[Any],
    field_descriptor: FieldDescriptor,
    validity_mask: Optional[Sequence[bool]],
    config: ProtarrowConfig,
) -> pa.Array:
    converter = _get_converter(field_descriptor, config)

    if converter is not None:
        data_type = field_descriptor_to_data_type(field_descriptor, config)
        null_value = (
            None
            if (
                field_descriptor.type == FieldDescriptor.TYPE_MESSAGE
                # We use none for repeated field as there should not
                # be any missing list elements, they are not nullable
                or field_descriptor.label == FieldDescriptor.LABEL_REPEATED
            )
            else converter(field_descriptor.default_value)
        )
        array = []
        for i, record in enumerate(proto_field_values):
            if record is None or not (validity_mask is None or validity_mask[i]):
                value = null_value
            else:
                value = converter(record)
            array.append(value)
        return pa.array(array, data_type)
    else:
        return _messages_to_array(
            proto_field_values,
            field_descriptor.message_type,
            validity_mask=validity_mask,
            config=config,
        )


def _get_offsets(
    repeated_values: Iterable[Union[RepeatedScalarFieldContainer, MessageMap]]
) -> List[int]:
    last_offset = 0
    offsets = []
    for record in repeated_values:
        if record is None:
            offsets.append(None)
        else:
            offsets.append(last_offset)
            last_offset += len(record)
    offsets.append(last_offset)
    return offsets


def _repeated_proto_to_array(
    repeated_values: Iterable[RepeatedScalarFieldContainer],
    field_descriptor: FieldDescriptor,
    config: ProtarrowConfig,
) -> pa.ListArray:
    """
    Convert Protobuf embedded lists to a 1-dimensional PyArrow ListArray with offsets
    See PyArrow Layout format documentation on how to calculate offsets.
    """
    offsets = _get_offsets(repeated_values)
    array = _proto_field_to_array(
        FlattenedIterable(repeated_values), field_descriptor, None, config
    )
    return pa.ListArray.from_arrays(
        offsets,
        array,
        pa.list_(
            pa.field(
                config.list_value_name, array.type, nullable=config.list_value_nullable
            )
        ),
    )


def _proto_map_to_array(
    maps: Iterable[MessageMap],
    field_descriptor: FieldDescriptor,
    config: ProtarrowConfig = ProtarrowConfig(),
) -> pa.MapArray:
    """
    Convert Protobuf maps to a 1-dimensional PyArrow MapArray with offsets
    See PyArrow Layout format documentation on how to calculate offsets.
    """
    key_field = field_descriptor.message_type.fields_by_name["key"]
    value_field = field_descriptor.message_type.fields_by_name["value"]
    offsets = _get_offsets(maps)
    keys = _proto_field_to_array(
        MapKeyIterable(maps),
        key_field,
        validity_mask=None,
        config=config,
    )
    values = _proto_field_to_array(
        MapValueIterable(maps),
        value_field,
        validity_mask=None,
        config=config,
    )
    return pa.MapArray.from_arrays(offsets, keys, values).cast(
        pa.map_(
            keys.type,
            pa.field(
                config.map_value_name, values.type, nullable=config.map_value_nullable
            ),
        )
    )


def _proto_field_nullable(
    field_descriptor: FieldDescriptor, config: ProtarrowConfig
) -> bool:
    if is_map(field_descriptor):
        return config.map_nullable
    elif field_descriptor.label == FieldDescriptorProto.LABEL_REPEATED:
        return config.list_nullable
    else:
        return field_descriptor.type == FieldDescriptorProto.TYPE_MESSAGE


def _proto_field_validity_mask(
    messages: Iterable[Message], field_descriptor: FieldDescriptor
) -> Optional[List[bool]]:
    if (
        field_descriptor.type != FieldDescriptorProto.TYPE_MESSAGE
        or field_descriptor.label == FieldDescriptorProto.LABEL_REPEATED
    ):
        return None
    mask = []
    field_name = field_descriptor.name
    for record in messages:
        if record is None:
            mask.append(False)
        else:
            mask.append(record.HasField(field_name))
    return mask


def _messages_to_array(
    messages: Iterable[Message],
    descriptor: Descriptor,
    validity_mask: Optional[Sequence[bool]],
    config: ProtarrowConfig,
) -> pa.StructArray:
    arrays = []
    fields = []

    for field_descriptor in descriptor.fields:
        if (
            field_descriptor.type == FieldDescriptor.TYPE_MESSAGE
            and field_descriptor.label != FieldDescriptor.LABEL_REPEATED
        ):
            field_values = NestedIterable(
                messages, NestedMessageGetter(field_descriptor.name)
            )
        else:
            field_values = NestedIterable(
                messages, operator.attrgetter(field_descriptor.name)
            )
        if is_map(field_descriptor):
            array = _proto_map_to_array(field_values, field_descriptor, config)
        elif field_descriptor.label == FieldDescriptorProto.LABEL_REPEATED:
            array = _repeated_proto_to_array(field_values, field_descriptor, config)
        else:
            mask = _proto_field_validity_mask(messages, field_descriptor)
            array = _proto_field_to_array(
                field_values, field_descriptor, validity_mask=mask, config=config
            )

        arrays.append(array)
        fields.append(
            pa.field(
                field_descriptor.name,
                array.type,
                nullable=_proto_field_nullable(field_descriptor, config),
            )
        )
    if len(arrays) == 0:
        # TODO: remove when this is fixed
        #  https://github.com/apache/arrow/issues/15109
        size = len(validity_mask) if validity_mask else len(messages)
        return pa.StructArray.from_arrays(
            arrays=[pa.nulls(size, pa.null())],
            fields=[pa.field("null", pa.null())],
            mask=pc.invert(pa.array(validity_mask, pa.bool_()))
            if validity_mask
            else None,
        ).cast(pa.struct([]))
    else:
        return pa.StructArray.from_arrays(
            arrays=arrays,
            fields=fields,
            mask=pc.invert(pa.array(validity_mask, pa.bool_()))
            if validity_mask
            else None,
        )


def messages_to_record_batch(
    messages: Iterable[M],
    message_type: Type[M],
    config: ProtarrowConfig = ProtarrowConfig(),
):
    return pa.RecordBatch.from_struct_array(
        _messages_to_array(
            messages,
            message_type.DESCRIPTOR,
            validity_mask=None,
            config=config,
        )
    )


def messages_to_table(
    messages: Iterable[M],
    message_type: Type[M],
    config: ProtarrowConfig = ProtarrowConfig(),
) -> pa.Table:
    """Converts a list of protobuf messages to a `pa.Table`"""
    assert isinstance(config, ProtarrowConfig), config
    record_batch = messages_to_record_batch(messages, message_type, config=config)
    return pa.Table.from_batches([record_batch])


def message_type_to_schema(
    message_type: Type[Message],
    config: ProtarrowConfig = ProtarrowConfig(),
) -> pa.Schema:
    return pa.schema(
        [
            field_descriptor_to_field(field_descriptor, config)
            for field_descriptor in message_type.DESCRIPTOR.fields
        ]
    )


def message_type_to_struct_type(
    message_type: Type[Message], config: ProtarrowConfig = ProtarrowConfig()
) -> pa.StructType:
    return pa.struct(
        [
            field_descriptor_to_field(field_descriptor, config)
            for field_descriptor in message_type.DESCRIPTOR.fields
        ]
    )
