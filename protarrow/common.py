import dataclasses
from typing import Optional, TypeVar, Union

import pyarrow as pa
from google.protobuf.message import Message

M = TypeVar("M", bound=Message)

SUPPORTED_ENUM_TYPES = (
    pa.int32(),
    pa.binary(),
    pa.large_binary(),
    pa.string(),
    pa.large_string(),
    pa.dictionary(pa.int32(), pa.string()),
    pa.dictionary(pa.int32(), pa.large_string()),
    pa.dictionary(pa.int32(), pa.binary()),
    pa.dictionary(pa.int32(), pa.large_binary()),
)

SUPPORTED_LIST_ARRAY_TYPES = (pa.ListArray, pa.LargeListArray)
SUPPORTED_STRING_TYPES = (pa.string(), pa.large_string())
SUPPORTED_BINARY_TYPES = (pa.binary(), pa.large_binary())


@dataclasses.dataclass(frozen=True)
class ProtarrowConfig:
    enum_type: pa.DataType = pa.int32()
    timestamp_type: pa.TimestampType = pa.timestamp("ns", "UTC")
    time_of_day_type: Union[pa.Time64Type, pa.Time32Type] = pa.time64("ns")
    duration_type: pa.DurationType = pa.duration("ns")
    list_nullable: bool = False
    map_nullable: bool = False
    list_value_nullable: bool = False
    map_value_nullable: bool = False
    list_value_name: str = "item"
    map_value_name: str = "value"
    field_number_key: Optional[bytes] = None
    string_type: pa.DataType = pa.string()
    binary_type: pa.DataType = pa.binary()
    list_array_type: type = pa.ListArray
    purge_cyclical_messages: bool = False

    def __post_init__(self):
        assert self.enum_type in SUPPORTED_ENUM_TYPES
        assert isinstance(self.field_number_key, (bytes, type(None)))
        assert self.string_type in SUPPORTED_STRING_TYPES
        assert self.binary_type in SUPPORTED_BINARY_TYPES
        assert self.list_array_type in SUPPORTED_LIST_ARRAY_TYPES

    def field_metadata(self, field_number: int) -> Optional[dict[bytes, bytes]]:
        if self.field_number_key is None:
            return None
        else:
            return {self.field_number_key: str(field_number).encode("utf-8")}

    def list_(self, item_type: pa.DataType) -> pa.DataType:
        return (pa.list_ if self.list_array_type is pa.ListArray else pa.large_list)(
            pa.field(
                self.list_value_name, item_type, nullable=self.list_value_nullable
            ),
        )


def is_binary_enum(data_type: pa.DataType) -> bool:
    return pa.types.is_binary(data_type) or (
        pa.types.is_dictionary(data_type) and pa.types.is_binary(data_type.value_type)
    )


def is_string_enum(data_type: pa.DataType) -> bool:
    return pa.types.is_string(data_type) or (
        pa.types.is_dictionary(data_type) and pa.types.is_string(data_type.value_type)
    )


def offset_values_array(
    array: Union[pa.ListArray, pa.MapArray], values_array: pa.Array
) -> pa.Array:
    """Apply the ListArray/MapArray offset to its child value array"""
    if array.offset == 0 or len(array.offsets) == 0:
        return values_array
    else:
        return values_array[array.offsets[0].as_py() :]
