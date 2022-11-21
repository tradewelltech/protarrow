# Protarrow

A library for converting from protobuf to arrow and back 

# Installation

```shell
pip install protarrow
```

# Usage

## Convert from proto to arrow

```protobuf
message MyProto {
  string name = 1;
  repeated int32 values = 2;
}
```

```python
import protarrow

my_protos = [
    MyProto(name="foo", values=[1, 2, 4]),
    MyProto(name="bar", values=[1, 2, 4]),
]

schema = protarrow.message_type_to_schema(MyProto)
record_batch = protarrow.messages_to_record_batch(my_protos, MyProto)
table = protarrow.messages_to_table(my_protos, MyProto)
```

| name   | values   |
|:-------|:---------|
| foo    | [1 2 4]  |
| bar    | [3 4 5]  |


## Convert from arrow to proto

```python
protos_from_record_batch = protarrow.table_to_messages(record_batch, MyProto)
protos_from_table = protarrow.table_to_messages(table, MyProto)
```

## Customize arrow type

The arrow type for enum and timestamps can be configured:

```python
config = protarrow.ProtarrowConfig(enum_type=pa.int32())
config = protarrow.ProtarrowConfig(
    timestamp_type=pa.timestamp("ms", "America/New_York")
)
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


| Proto                       | Pyarrow                | Note                               |
|-----------------------------|------------------------|------------------------------------|
| repeated                    | list_                  |                                    |
| map                         | map_                   |                                    |
| google.protobuf.BoolValue   | bool_                  |                                    |
| google.protobuf.BytesValue  | binary                 |                                    |
| google.protobuf.DoubleValue | float64                |                                    |
| google.protobuf.FloatValue  | float32                |                                    |
| google.protobuf.Int32Value  | int32                  |                                    |
| google.protobuf.Int64Value  | int64                  |                                    |
| google.protobuf.StringValue | string                 |                                    |
| google.protobuf.Timestamp   | timestamp("ns", "UTC") | Unit and timezone are configurable |
| google.protobuf.UInt32Value | uint32                 |                                    |
| google.protobuf.UInt64Value | uint64                 |                                    |
| google.type.Date            | date32()               |                                    |
| google.type.TimeOfDay       | time64("ns")           |                                    |

## Nullability

* Top level native field, list and maps are marked as non-nullable.
* Any nested message and their children are nullable

# Development

## Set up

```shell
python3 -m venv --clear venv
source venv/bin/activate
poetry install
python ./scripts/protoc.py
```

## Testing

This library relies on property based testing. 
Tests convert randomly generated data from protobuf to arrow and back, making sure the end result is the same as the input.

```shell
coverage run --branch --include "*/protarrow/*" -m pytest tests
coverage report
```
