import collections.abc
import dataclasses
import datetime
from typing import Any, Callable, Iterable, Iterator, List, Optional, Type

import pyarrow as pa
from google.protobuf.descriptor import Descriptor, EnumDescriptor, FieldDescriptor
from google.protobuf.internal.containers import MessageMap
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

from protarrow.common import M, is_binary_enum, is_string_enum

_NANOS_PER_UNIT = {"ns": 1, "us": 1_000, "ms": 1_000_000, "s": 1_000_000_000}
_TIME_CONVERTER = {
    pa.time64("ns"): 1,
    pa.time64("us"): 1_000,
    pa.time32("ms"): 1_000_000,
    pa.time32("s"): 1_000_000_00,
}


def _timestamp_ns_scalar_to_proto(scalar: pa.TimestampScalar) -> Timestamp:
    timestamp = Timestamp()
    timestamp.FromNanoseconds(scalar.value)
    return timestamp


def _timestamp_us_scalar_to_proto(scalar: pa.TimestampScalar) -> Timestamp:
    timestamp = Timestamp()
    timestamp.FromMicroseconds(scalar.value)
    return timestamp


def _timestamp_ms_scalar_to_proto(scalar: pa.TimestampScalar) -> Timestamp:
    timestamp = Timestamp()
    timestamp.FromMilliseconds(scalar.value)
    return timestamp


def _timestamp_s_scalar_to_proto(scalar: pa.TimestampScalar) -> Timestamp:
    timestamp = Timestamp()
    timestamp.FromSeconds(scalar.value)
    return timestamp


def _date_scalar_to_proto(scalar: pa.Date32Scalar) -> Date:
    date: datetime.date = scalar.as_py()
    if date == datetime.date.min:
        return Date()
    else:
        return Date(year=date.year, month=date.month, day=date.day)


def _time_64_ns_scalar_to_proto(scalar: pa.Time64Scalar) -> TimeOfDay:
    total_nanos = scalar.cast(pa.int64()).as_py()
    return TimeOfDay(
        nanos=total_nanos % 1_000_000_000,
        seconds=(total_nanos // 1_000_000_000) % 60,
        minutes=(total_nanos // 60_000_000_000) % 60,
        hours=(total_nanos // 3600_000_000_000),
    )


def _time_64_us_scalar_to_proto(scalar: pa.Time64Scalar) -> TimeOfDay:
    total_us = scalar.cast(pa.int64()).as_py()
    return TimeOfDay(
        nanos=(total_us % 1_000_000) * 1_000,
        seconds=(total_us // 1_000_000) % 60,
        minutes=(total_us // 60_000_000) % 60,
        hours=(total_us // 3600_000_000),
    )


def _time_32_ms_scalar_to_proto(scalar: pa.Time32Scalar) -> TimeOfDay:
    total_ms = scalar.cast(pa.int32()).as_py()
    return TimeOfDay(
        nanos=(total_ms % 1_000) * 1_000_000,
        seconds=(total_ms // 1_000) % 60,
        minutes=(total_ms // 60_000) % 60,
        hours=(total_ms // 3600_000),
    )


def _time_32_s_scalar_to_proto(scalar: pa.Time32Scalar) -> TimeOfDay:
    total_s = scalar.cast(pa.int32()).as_py()
    return TimeOfDay(
        nanos=0,
        seconds=(total_s // 1) % 60,
        minutes=(total_s // 60) % 60,
        hours=(total_s // 3600),
    )


TIME_OF_DAY_CONVERTERS = {
    pa.time64("ns"): _time_64_ns_scalar_to_proto,
    pa.time64("us"): _time_64_us_scalar_to_proto,
    pa.time32("ms"): _time_32_ms_scalar_to_proto,
    pa.time32("s"): _time_32_s_scalar_to_proto,
}

TIMESTAMP_CONVERTERS = {
    "ns": _timestamp_ns_scalar_to_proto,
    "us": _timestamp_us_scalar_to_proto,
    "ms": _timestamp_ms_scalar_to_proto,
    "s": _timestamp_s_scalar_to_proto,
}

TEMPORAL_CONVERTERS = {
    Timestamp.DESCRIPTOR: lambda data_type: TIMESTAMP_CONVERTERS[data_type.unit],
    Date.DESCRIPTOR: lambda _: _date_scalar_to_proto,
    TimeOfDay.DESCRIPTOR: TIME_OF_DAY_CONVERTERS.__getitem__,
}

NULLABLE_TYPES = (
    BoolValue.DESCRIPTOR,
    BytesValue.DESCRIPTOR,
    DoubleValue.DESCRIPTOR,
    FloatValue.DESCRIPTOR,
    Int32Value.DESCRIPTOR,
    Int64Value.DESCRIPTOR,
    StringValue.DESCRIPTOR,
    UInt32Value.DESCRIPTOR,
    UInt64Value.DESCRIPTOR,
)

SPECIAL_CONVERTERS = {
    **TEMPORAL_CONVERTERS,
    **{
        nullable_descriptor: lambda _: convert_scalar
        for nullable_descriptor in NULLABLE_TYPES
    },
}


def is_custom_field(field_descriptor: FieldDescriptor):
    return (
        field_descriptor.type == FieldDescriptor.TYPE_MESSAGE
        and field_descriptor.message_type not in SPECIAL_CONVERTERS
    )


@dataclasses.dataclass(frozen=True)
class OffsetToSize(collections.abc.Iterable):
    array: pa.Array

    def __post_init__(self):
        assert pa.types.is_integer(self.array.type)

    def __iter__(self) -> Iterator[int]:
        if len(self.array) > 0:
            current_offset = self.array[0].as_py()
            for item in self.array[1:]:
                offset = item.as_py()
                yield offset - current_offset
                current_offset = offset


@dataclasses.dataclass(frozen=True)
class OptionalNestedIterable(collections.abc.Iterable):
    parents: Iterable[Message]
    field_descriptor: FieldDescriptor
    validity_mask: Iterable[pa.BooleanScalar]

    def __iter__(self) -> Iterator[Optional[Any]]:
        for parent, valid in zip(self.parents, self.validity_mask):
            if valid.is_valid and valid.as_py():
                yield getattr(parent, self.field_descriptor.name)
            else:
                yield None

    def prime(self):
        """This needs to be called if there are no fields in the message"""
        empty = self.field_descriptor.message_type._concrete_class()
        for parent, valid in zip(self.parents, self.validity_mask):
            if valid.is_valid and valid.as_py():
                value = getattr(parent, self.field_descriptor.name)
                value.MergeFrom(empty)


@dataclasses.dataclass(frozen=True)
class RepeatedNestedIterable(collections.abc.Iterable):
    parents: Iterable[Message]
    field_descriptor: FieldDescriptor

    def __post_init__(self):
        assert self.field_descriptor.label == FieldDescriptor.LABEL_REPEATED
        assert self.field_descriptor.type == FieldDescriptor.TYPE_MESSAGE

    def __iter__(self) -> Iterator[Any]:
        for parent in self.parents:
            for child in getattr(parent, self.field_descriptor.name):
                yield child


def convert_scalar(scalar: pa.Scalar) -> Any:
    return scalar.as_py()


def create_enum_converter(
    enum_descriptor: EnumDescriptor, arrow_type: pa.DataType
) -> Callable[[pa.Scalar], int]:
    if pa.types.is_integer(arrow_type):
        return lambda x: x.as_py()
    elif is_binary_enum(arrow_type):
        mapping = {v.name.encode("utf-8"): v.number for v in enum_descriptor.values}
        return lambda x: mapping.get(x.as_py(), 0)
    elif is_string_enum(arrow_type):
        mapping = {v.name: v.number for v in enum_descriptor.values}
        return lambda x: mapping.get(x.as_py(), 0)
    else:
        raise TypeError(arrow_type)


def get_converter(
    field_descriptor: FieldDescriptor, arrow_type: pa.DataType
) -> Callable[[pa.Scalar], Any]:
    if field_descriptor.type == FieldDescriptor.TYPE_ENUM:
        enum_descriptor: EnumDescriptor = field_descriptor.enum_type
        return create_enum_converter(enum_descriptor, arrow_type)
    elif field_descriptor.type == FieldDescriptor.TYPE_MESSAGE:
        return SPECIAL_CONVERTERS[field_descriptor.message_type](arrow_type)
    else:
        return convert_scalar


class PlainAssigner(collections.abc.Iterable):
    def __init__(
        self,
        messages: Iterable[Message],
        field_descriptor: FieldDescriptor,
        arrow_type: pa.DataType,
    ):
        self.messages = messages
        self.field_descriptor = field_descriptor
        self.converter = get_converter(field_descriptor, arrow_type)
        self.nullable = self.field_descriptor.message_type in NULLABLE_TYPES
        self.message = None

    def __iter__(self) -> Iterator[Callable[[pa.Scalar], None]]:
        assert self.message is None
        for message in self.messages:
            self.message = message
            yield self
        self.message = None

    def __call__(self, scalar: pa.Scalar) -> None:
        value = self.converter(scalar) if scalar.is_valid else None
        if value is not None:
            if self.nullable:
                getattr(self.message, self.field_descriptor.name).value = value
            else:
                setattr(self.message, self.field_descriptor.name, value)


class AppendAssigner(collections.abc.Iterable):
    def __init__(
        self,
        messages: Iterable[Message],
        field_descriptor: FieldDescriptor,
        sizes: Iterable[int],
        converter: Callable[[Any], Any],
    ):
        self.messages = messages
        self.field_descriptor = field_descriptor
        assert self.field_descriptor.label == FieldDescriptor.LABEL_REPEATED
        self.sizes = sizes
        self.converter = converter
        self.attribute = None

    def __iter__(self) -> Iterator[Callable[[pa.Scalar], None]]:
        assert self.attribute is None
        for message, size in zip(self.messages, self.sizes):
            self.attribute = getattr(message, self.field_descriptor.name)
            for _ in range(size):
                yield self
        self.attribute = None

    def __call__(self, scalar: pa.Scalar) -> None:
        self.attribute.append(self.converter(scalar))


@dataclasses.dataclass
class MapKeyAssigner(collections.abc.Iterable):
    messages: Iterable[Message]
    field_descriptor: FieldDescriptor
    key_arrow_type: dataclasses.InitVar[pa.DataType]
    sizes: Iterable[int]
    converter: Callable[[pa.Scalar], Any] = dataclasses.field(init=False)
    attribute: Any = None

    def __post_init__(self, key_arrow_type: pa.DataType):
        assert self.field_descriptor.label == FieldDescriptor.LABEL_REPEATED
        assert self.field_descriptor.message_type.GetOptions().map_entry
        self.converter = get_converter(
            self.field_descriptor.message_type.fields_by_name["key"], key_arrow_type
        )

    def __iter__(self) -> Iterator[Callable[[pa.Scalar], Message]]:
        assert self.attribute is None
        for message, offset in zip(self.messages, self.sizes):
            self.attribute = getattr(message, self.field_descriptor.name)
            for _ in range(offset):
                yield self
        self.attribute = None

    def __call__(self, scalar: pa.Scalar) -> Message:
        return self.attribute[self.converter(scalar)]


def _direct_assign_map(attribute: MessageMap, key: Any, value: Any):
    attribute[key] = value


def _merge_assign_map(attribute: MessageMap, key: Any, value: Any):
    if value is None:
        attribute[key]
    else:
        attribute[key].MergeFrom(value)


@dataclasses.dataclass
class MapItemAssigner(collections.abc.Iterable):
    messages: Iterable[Message]
    field_descriptor: FieldDescriptor
    key_arrow_type: dataclasses.InitVar[pa.DataType]
    value_arrow_type: dataclasses.InitVar[pa.DataType]
    sizes: Iterable[int]
    key_converter: Callable[[pa.Scalar], Any] = dataclasses.field(init=False)
    value_converter: Callable[[pa.Scalar], Any] = dataclasses.field(init=False)
    assigner: Callable[[MessageMap, Any, Any], None] = dataclasses.field(init=False)
    attribute: Optional[MessageMap] = None

    def __post_init__(self, key_arrow_type: pa.DataType, value_arrow_type: pa.DataType):
        assert self.field_descriptor.label == FieldDescriptor.LABEL_REPEATED
        assert self.field_descriptor.message_type.GetOptions().map_entry
        self.key_converter = get_converter(
            self.field_descriptor.message_type.fields_by_name["key"], key_arrow_type
        )
        value_descriptor = self.field_descriptor.message_type.fields_by_name["value"]
        self.value_converter = WrappedValueConverterAdapter.maybe_wrap(
            get_converter(value_descriptor, value_arrow_type), value_descriptor
        )
        self.assigner = (
            _merge_assign_map
            if (value_descriptor.type == FieldDescriptor.TYPE_MESSAGE)
            else _direct_assign_map
        )

    def __iter__(self) -> Iterator[Callable[[pa.Scalar, pa.Scalar], Message]]:
        assert self.attribute is None
        for message, size in zip(self.messages, self.sizes):
            self.attribute = getattr(message, self.field_descriptor.name)
            for _ in range(size):
                yield self
        self.attribute = None

    def __call__(self, key: pa.Scalar, value: pa.Scalar):
        self.assigner(
            self.attribute,
            self.key_converter(key),
            self.value_converter(value) if value.is_valid else None,
        )


def _extract_struct_field(
    array: pa.StructArray,
    field_descriptor: FieldDescriptor,
    messages: Iterable[Message],
) -> None:
    nested_list = OptionalNestedIterable(messages, field_descriptor, array.is_valid())
    nested_list.prime()
    _extract_array_messages(array, field_descriptor.message_type, nested_list)


def _extract_map_field(
    array: pa.MapArray,
    field_descriptor: FieldDescriptor,
    messages: Iterable[Message],
) -> None:
    assert pa.types.is_map(array.type), array.type
    value_descriptor = field_descriptor.message_type.fields_by_name["value"]

    if is_custom_field(value_descriptor):
        # Because protobuf doesn't warranty orders of map,
        # we have to make a copy of the list of values here
        values = []
        for assigner, key in zip(
            MapKeyAssigner(
                messages,
                field_descriptor,
                array.type.key_type,
                OffsetToSize(array.offsets),
            ),
            array.keys,
        ):
            values.append(assigner(key))

        assert pa.types.is_struct(array.type.item_type), array.type
        item_type: pa.StructType = array.type.item_type
        assert isinstance(item_type, pa.StructType)

        for field_descriptor in value_descriptor.message_type.fields:
            field_index = item_type.get_field_index(field_descriptor.name)
            if field_index != -1:
                _extract_field(
                    array.values.field(1).field(field_index),
                    field_descriptor,
                    values,
                )

    else:
        for assigner, key, value in zip(
            MapItemAssigner(
                messages,
                field_descriptor,
                array.type.key_type,
                array.type.item_type,
                OffsetToSize(array.offsets),
            ),
            array.keys,
            array.values.field(1),
        ):
            assigner(key, value)


def _extract_repeated_field(
    array: pa.Array,
    field_descriptor: FieldDescriptor,
    messages: Iterable[Message],
) -> None:
    if is_custom_field(field_descriptor):
        if field_descriptor.message_type.GetOptions().map_entry:
            _extract_map_field(array, field_descriptor, messages)
        else:
            _extract_repeated_message(array, field_descriptor, messages)
    else:
        _extract_repeated_primitive(array, field_descriptor, messages)


@dataclasses.dataclass(frozen=True)
class WrappedValueConverterAdapter:
    converter: Callable[[pa.Scalar], Any]
    wrapped_type: type

    def __call__(self, scalar: pa.Scalar):
        return self.wrapped_type(value=self.converter(scalar))

    @staticmethod
    def maybe_wrap(
        converter: Callable[[pa.Scalar], Any], field_descriptor: FieldDescriptor
    ) -> Callable[[pa.Scalar], Any]:
        if field_descriptor.message_type in NULLABLE_TYPES:
            return WrappedValueConverterAdapter(
                converter, field_descriptor.message_type._concrete_class
            )
        else:
            return converter


def _extract_repeated_primitive(
    array: pa.Array, field_descriptor: FieldDescriptor, messages: Iterable[Message]
) -> None:
    converter = WrappedValueConverterAdapter.maybe_wrap(
        get_converter(field_descriptor, array.type.value_type), field_descriptor
    )
    assigner = AppendAssigner(
        messages=messages,
        field_descriptor=field_descriptor,
        sizes=OffsetToSize(array.offsets),
        converter=converter,
    )

    for each_assigner, value in zip(assigner, array.values):
        each_assigner(value)


def _extract_repeated_message(
    array: pa.Array, field_descriptor: FieldDescriptor, messages: Iterable[Message]
):
    assert pa.types.is_list(array.type)
    child = field_descriptor.message_type._concrete_class()
    assigner = AppendAssigner(
        messages,
        field_descriptor,
        OffsetToSize(array.offsets),
        lambda x: x,
    )
    for each_assigner, value in zip(assigner, array.values):
        each_assigner(child)
    _extract_array_messages(
        array.values,
        field_descriptor.message_type,
        RepeatedNestedIterable(messages, field_descriptor),
    )


def _extract_field(
    array: pa.Array, field_descriptor: FieldDescriptor, messages: Iterable[Message]
) -> None:
    if field_descriptor.label == FieldDescriptor.LABEL_REPEATED:
        _extract_repeated_field(array, field_descriptor, messages)
    elif field_descriptor.message_type in TEMPORAL_CONVERTERS:
        extractor = TEMPORAL_CONVERTERS[field_descriptor.message_type](array.type)
        for message, value in zip(messages, array):
            if value.is_valid:
                getattr(
                    message,
                    field_descriptor.name,
                ).MergeFrom(extractor(value))
    elif (
        field_descriptor.type == FieldDescriptor.TYPE_MESSAGE
        and field_descriptor.message_type not in NULLABLE_TYPES
    ):
        _extract_struct_field(array, field_descriptor, messages)
    else:
        plain_assigner = PlainAssigner(messages, field_descriptor, array.type)
        for plain_assigner, value in zip(plain_assigner, array):
            if value.is_valid:
                plain_assigner(value)


def _extract_record_batch_messages(
    record_batch: pa.RecordBatch,
    message_descriptor: Descriptor,
    messages: Iterable[Message],
) -> None:
    for field_descriptor in message_descriptor.fields:
        if field_descriptor.name in record_batch.schema.names:
            _extract_field(
                record_batch[field_descriptor.name], field_descriptor, messages
            )


def _extract_array_messages(
    array: pa.StructArray,
    message_descriptor: Descriptor,
    messages: Iterable[Message],
) -> None:
    assert pa.types.is_struct(array.type), array.type
    assert isinstance(array, pa.StructArray)
    struct_type: pa.StructType = array.type
    for field_descriptor in message_descriptor.fields:
        index = struct_type.get_field_index(field_descriptor.name)
        if index != -1:
            _extract_field(array.field(index), field_descriptor, messages)


def record_batch_to_messages(
    record_batch: pa.RecordBatch, message_type: Type[M]
) -> List[M]:
    messages = [message_type() for _ in range(record_batch.num_rows)]
    _extract_record_batch_messages(record_batch, message_type.DESCRIPTOR, messages)
    return messages


def table_to_messages(table: pa.Table, message_type: Type[M]) -> List[M]:
    messages = []
    for batch in table.to_reader():
        messages.extend(record_batch_to_messages(batch, message_type))
    return messages
