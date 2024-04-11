import datetime
import random
import secrets
import typing

from google.protobuf.descriptor import EnumDescriptor, FieldDescriptor
from google.protobuf.message import Message
from google.protobuf.timestamp_pb2 import Timestamp
from google.type.date_pb2 import Date
from google.type.timeofday_pb2 import TimeOfDay

from protarrow.common import M
from protarrow.proto_to_arrow import is_map

EPOCH_RATIO = 24 * 60 * 60

UNIT_IN_NANOS = {"s": 1_000_000_000, "ms": 1_000_000, "us": 1_000, "ns": 1}


def random_string(count: int) -> str:
    return secrets.token_urlsafe(random.randint(0, count))


def random_bytes(count: int) -> bytes:
    return secrets.token_bytes(random.randint(0, count))


def random_timestamp() -> Timestamp:
    return Timestamp(
        seconds=random.randint(-9223372036, 9223372035),
        nanos=random.randint(0, 999_999_999),
    )


def random_date() -> Date:
    date = datetime.date.min + datetime.timedelta(days=random.randint(0, 3652058))
    return Date(year=date.year, month=date.month, day=date.day)


def random_time_of_day() -> TimeOfDay:
    return TimeOfDay(
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
        nanos=random.randint(0, 999_999_999),
    )


CPP_TYPE_GENERATOR = {
    FieldDescriptor.CPPTYPE_INT32: lambda: random.randint(-(2**31), 2**31 - 1),
    FieldDescriptor.CPPTYPE_INT64: lambda: random.randint(-(2**63), 2**63 - 1),
    FieldDescriptor.CPPTYPE_UINT32: lambda: random.randint(0, 2**32 - 1),
    FieldDescriptor.CPPTYPE_UINT64: lambda: random.randint(0, 2**64 - 1),
    FieldDescriptor.CPPTYPE_DOUBLE: lambda: random.uniform(-1, 1),
    FieldDescriptor.CPPTYPE_FLOAT: lambda: random.uniform(-1, 1),
    FieldDescriptor.CPPTYPE_BOOL: lambda: bool(random.getrandbits(1)),
}

TYPE_GENERATOR = {
    FieldDescriptor.TYPE_BYTES: random_bytes,
    FieldDescriptor.TYPE_STRING: random_string,
}

MESSAGE_GENERATORS = {
    Date.DESCRIPTOR: random_date,
    Timestamp.DESCRIPTOR: random_timestamp,
    TimeOfDay.DESCRIPTOR: random_time_of_day,
}


def generate_message(message_type: typing.Type[M], repeated_count: int) -> M:
    message = message_type()
    for one_of in message_type.DESCRIPTOR.oneofs:
        one_of_index = random.randint(0, len(one_of.fields))
        if one_of_index < len(one_of.fields):
            field = one_of.fields[one_of_index]
            set_field(message, field, repeated_count)

    for field in message_type.DESCRIPTOR.fields:
        if field.containing_oneof is None:
            if (
                field.label == FieldDescriptor.LABEL_REPEATED
                or field.type != FieldDescriptor.TYPE_MESSAGE
                or random.getrandbits(1) == 1
            ):
                set_field(message, field, repeated_count)
    return message


def generate_messages(
    message_type: typing.Type[M], count: int, repeated_count: int = 10
) -> typing.List[M]:
    return [generate_message(message_type, repeated_count) for _ in range(count)]


def set_field(message: Message, field: FieldDescriptor, count: int) -> None:
    data = generate_field_data(field, count)

    if field.label == FieldDescriptor.LABEL_REPEATED:
        field_value = getattr(message, field.name)
        if is_map(field):
            if (
                field.message_type.fields_by_name["value"].type
                == FieldDescriptor.TYPE_MESSAGE
            ):
                for entry in data:
                    field_value[entry.key].MergeFrom(entry.value)
            else:
                for entry in data:
                    field_value[entry.key] = entry.value
        else:
            field_value.extend(data)
    elif field.has_presence:
        if random.getrandbits(1) == 1:
            if field.type == FieldDescriptor.TYPE_MESSAGE:
                getattr(message, field.name).CopyFrom(data)
            else:
                setattr(message, field.name, data)
    else:
        setattr(message, field.name, data)


def generate_field_data(field: FieldDescriptor, count: int):
    if field.label == FieldDescriptor.LABEL_REPEATED:
        size = random.randint(0, count)
        return [_generate_data(field, count) for _ in range(size)]
    else:
        return _generate_data(field, count)


def _generate_data(field: FieldDescriptor, count: int) -> typing.Any:
    if field.type == FieldDescriptor.TYPE_ENUM:
        return _generate_enum(field.enum_type)
    elif field.message_type in MESSAGE_GENERATORS:
        return MESSAGE_GENERATORS[field.message_type]()
    elif field.type == FieldDescriptor.TYPE_MESSAGE:
        return generate_message(field.message_type._concrete_class, count)
    elif field.type in TYPE_GENERATOR:
        return TYPE_GENERATOR[field.type](count)
    else:
        return CPP_TYPE_GENERATOR[field.cpp_type]()


def _generate_enum(enum: EnumDescriptor) -> int:
    return random.choice(enum.values).index


def truncate_nanos(message: Message, timestamp_unit: str, time_unit: str) -> Message:
    if message.DESCRIPTOR == Timestamp.DESCRIPTOR:
        message.nanos = (
            message.nanos // UNIT_IN_NANOS[timestamp_unit]
        ) * UNIT_IN_NANOS[timestamp_unit]
    elif message.DESCRIPTOR == TimeOfDay.DESCRIPTOR:
        message.nanos = (message.nanos // UNIT_IN_NANOS[time_unit]) * UNIT_IN_NANOS[
            time_unit
        ]
    else:
        for field in message.DESCRIPTOR.fields:
            if field.type == FieldDescriptor.TYPE_MESSAGE:
                if field.label == FieldDescriptor.LABEL_REPEATED:
                    field_value = getattr(message, field.name)
                    if (
                        field.message_type is not None
                        and field.message_type.GetOptions().map_entry
                    ):
                        if (
                            field.message_type.fields_by_name["value"].type
                            == FieldDescriptor.TYPE_MESSAGE
                        ):
                            for key, value in field_value.items():
                                truncate_nanos(value, timestamp_unit, time_unit)

                    else:
                        for item in field_value:
                            truncate_nanos(item, timestamp_unit, time_unit)
                elif message.HasField(field.name):
                    truncate_nanos(
                        getattr(message, field.name), timestamp_unit, time_unit
                    )
    return message
