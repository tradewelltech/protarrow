# Protarrow

A library for converting from protobuf to arrow and back 

# TLDR;

Take a protobuf message:

```protobuf
message MyProto {
  string name = 1;
  repeated int32 values = 2;
}
```

And convert from `google.protobuf.message.Message`s to `arrow.Table`, or vice versa
```python

from protarrow import messages_to_table, table_to_messages
from example_pb2 import MyProto

my_protos = [
    MyProto(name="foo", values=[1, 2, 4]),
    MyProto(name="bar", values=[1, 2, 4]),
]

table = messages_to_table(my_protos, MyProto)
protos_from_table = table_to_messages(table, MyProto)
```

# Type Mapping

## Native Types

| Proto    | Pyarrow                 | Note         |
|----------|-------------------------|--------------|
| bool     | bool_                   |              |
| bytes    | binary                  |              |
| double   | float64                 |              |
| enum     | **int32**/string/binary | configurable |
| fixed32  | int32                   |              |
| fixed64  | int64                   |              |
| float    | float32                 |              |
| group    |                         | No supported |
| int32    | int32                   |              |
| int64    | int64                   |              |
| message  | struct                  |              |
| sfixed32 | int32                   |              |
| sfixed64 | int64                   |              |
| sint32   | int32                   |              |
| sint64   | int64                   |              |
| string   | string                  |              |
| uint32   | uint32                  |              |
| uint64   | uint64                  |              |

## Other types


| Proto                       | Pyarrow                | Note                    |
|-----------------------------|------------------------|-------------------------|
| repeated                    | list_                  |                         |
| map                         | map_                   |                         |
| google.protobuf.BoolValue   | bool_                  |                         |
| google.protobuf.BytesValue  | binary                 |                         |
| google.protobuf.DoubleValue | float64                |                         |
| google.protobuf.FloatValue  | float32                |                         |
| google.protobuf.Int32Value  | int32                  |                         |
| google.protobuf.Int64Value  | int64                  |                         |
| google.protobuf.StringValue | string                 |                         |
| google.protobuf.Timestamp   | timestamp("ns", "UTC") | To be made configurable |
| google.protobuf.UInt32Value | uint32                 |                         |
| google.protobuf.UInt64Value | uint64                 |                         |
| google.type.Date            | date32()               |                         |
| google.type.TimeOfDay       | time64("ns")           |                         |




# Development

## Set up

```shell
python3 -m venv --clear venv
source venv/bin/activate
poetry install
python ./scripts/protoc.py
```
