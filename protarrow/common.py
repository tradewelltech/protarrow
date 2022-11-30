import dataclasses
from typing import TypeVar, Union

import pyarrow as pa
from google.protobuf.message import Message

M = TypeVar("M", bound=Message)

SUPPORTED_ENUM_TYPES = (
    pa.int32(),
    pa.binary(),
    pa.string(),
    pa.dictionary(pa.int32(), pa.string()),
    pa.dictionary(pa.int32(), pa.binary()),
)


@dataclasses.dataclass(frozen=True)
class ProtarrowConfig:
    enum_type: pa.DataType = pa.int32()
    timestamp_type: pa.TimestampType = pa.timestamp("ns", "UTC")
    time_of_day_type: Union[pa.Time64Type, pa.Time32Type] = pa.time64("ns")
    list_nullable: bool = False
    map_nullable: bool = False
    list_value_nullable: bool = False
    map_value_nullable: bool = False
    list_value_name: str = "item"
    map_value_name: str = "value"

    def __post_init__(self):
        assert self.enum_type in SUPPORTED_ENUM_TYPES


def is_binary_enum(data_type: pa.DataType) -> bool:
    return pa.types.is_binary(data_type) or (
        pa.types.is_dictionary(data_type) and pa.types.is_binary(data_type.value_type)
    )


def is_string_enum(data_type: pa.DataType) -> bool:
    return pa.types.is_string(data_type) or (
        pa.types.is_dictionary(data_type) and pa.types.is_string(data_type.value_type)
    )
