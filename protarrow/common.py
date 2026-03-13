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
    pa.dictionary(pa.int32(), pa.binary()),
)

SUPPORTED_LIST_ARRAY_TYPES = (pa.ListArray, pa.LargeListArray)
SUPPORTED_STRING_TYPES = (pa.string(), pa.large_string())
SUPPORTED_BINARY_TYPES = (pa.binary(), pa.large_binary())


def _validate_enum_type(
    enum_type: pa.DataType,
    string_type: pa.DataType,
    binary_type: pa.DataType,
) -> None:
    if enum_type not in SUPPORTED_ENUM_TYPES:
        raise ValueError(
            f"Unsupported enum_type {enum_type}, expected one of {SUPPORTED_ENUM_TYPES}"
        )
    if pa.types.is_dictionary(enum_type):
        return
    if is_string_enum(enum_type) and enum_type != string_type:
        raise ValueError(
            f"enum string type {enum_type} does not match string_type {string_type}"
        )
    if is_binary_enum(enum_type) and enum_type != binary_type:
        raise ValueError(
            f"enum binary type {enum_type} does not match binary_type {binary_type}"
        )


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
    skip_recursive_messages: bool = False

    def __post_init__(self):
        _validate_enum_type(self.enum_type, self.string_type, self.binary_type)
        if not isinstance(self.field_number_key, (bytes, type(None))):
            raise TypeError(
                f"field_number_key must be bytes or None,"
                f" got {type(self.field_number_key).__name__}"
            )
        if self.string_type not in SUPPORTED_STRING_TYPES:
            raise ValueError(
                f"Unsupported string_type {self.string_type},"
                f" expected one of {SUPPORTED_STRING_TYPES}"
            )
        if self.binary_type not in SUPPORTED_BINARY_TYPES:
            raise ValueError(
                f"Unsupported binary_type {self.binary_type},"
                f" expected one of {SUPPORTED_BINARY_TYPES}"
            )
        if self.list_array_type not in SUPPORTED_LIST_ARRAY_TYPES:
            raise ValueError(
                f"Unsupported list_array_type {self.list_array_type},"
                f" expected one of {SUPPORTED_LIST_ARRAY_TYPES}"
            )

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


def _is_any_binary(data_type: pa.DataType) -> bool:
    return pa.types.is_binary(data_type) or pa.types.is_large_binary(data_type)


def _is_any_string(data_type: pa.DataType) -> bool:
    return pa.types.is_string(data_type) or pa.types.is_large_string(data_type)


def is_binary_enum(data_type: pa.DataType) -> bool:
    return _is_any_binary(data_type) or (
        pa.types.is_dictionary(data_type) and _is_any_binary(data_type.value_type)
    )


def is_string_enum(data_type: pa.DataType) -> bool:
    return _is_any_string(data_type) or (
        pa.types.is_dictionary(data_type) and _is_any_string(data_type.value_type)
    )


def offset_values_array(
    array: Union[pa.ListArray, pa.MapArray], values_array: pa.Array
) -> pa.Array:
    """Apply the ListArray/MapArray offset to its child value array"""
    if array.offset == 0 or len(array.offsets) == 0:
        return values_array
    else:
        return values_array[array.offsets[0].as_py() :]
