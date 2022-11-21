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

    def __post_init__(self):
        assert self.enum_type in SUPPORTED_ENUM_TYPES
