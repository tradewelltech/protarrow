# Protarrow

A library for converting from protobuf to arrow and back 

# TLDR;

```python
from my_proto_pb2 import MyProto

import protarrow

messages = [
    MyProto(foo="Bar"),
    MyProto(hello="world"),
]
table = protarrow.messages_to_table(messages, MyProto)
messages_from_table = protarrow.table_to_messages(table, MyProto)
```

# Type Mapping

## Native Types

| Proto    | Pyarrow | Note                    |
|----------|---------|-------------------------|
| bool     | bool_   |                         |
| bytes    | binary  |                         |
| double   | float64 |                         |
| enum     | binary  | To be made configurable |
| fixed32  | int32   |                         |
| fixed64  | int64   |                         |
| float    | float32 |                         |
| group    |         | No supported            |
| int32    | int32   |                         |
| int64    | int64   |                         |
| message  | struct  |                         |
| sfixed32 | int32   |                         |
| sfixed64 | int64   |                         |
| sint32   | int32   |                         |
| sint64   | int64   |                         |
| string   | string  |                         |
| uint32   | uint32  |                         |
| uint64   | uint64  |                         |

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

## TODO:

* [ ] add pydoc to the 4 main functions
* [ ] save benchmark results
* [ ] add tests with imported messages
* [ ] have test automatically read all messages in protobuf descriptor
* [ ] make Timestamp unit configurable
* [ ] make TimeOfDay unit configurable
* [ ] make protobuf enum configurable
* [ ] make random data configurable and repatable
* [ ] support oneof
* [ ] add mypy and other linter
* [ ] publish library
* [ ] add test coverage
* [ ] run tests in github action
* [ ] use automatic versioning
* [ ] publish doc
