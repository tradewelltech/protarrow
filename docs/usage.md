
# Usage

## Installation

```shell
pip install protarrow
```

## Convert from proto to arrow


```protobuf
message MyProto {
  string name = 1;
  int32 id = 2;
  repeated int32 values = 3;
}
```


```python
import protarrow

my_protos = [
    MyProto(name="foo", id=1, values=[1, 2, 4]),
    MyProto(name="bar", id=2, values=[3, 4, 5]),
]

schema = protarrow.message_type_to_schema(MyProto)
struct_Type = protarrow.message_type_to_struct_type(MyProto)
record_batch = protarrow.messages_to_record_batch(my_protos, MyProto)
table = protarrow.messages_to_table(my_protos, MyProto)
```


| name   |   id | values   |
|:-------|-----:|:---------|
| foo    |    1 | [1 2 4]  |
| bar    |    2 | [3 4 5]  |


## Convert from arrow to proto in batch

```python
protos_from_record_batch = protarrow.record_batch_to_messages(record_batch, MyProto)
protos_from_table = protarrow.table_to_messages(table, MyProto)
```

## Convert from arrow to proto row by row

```python
message_extractor = protarrow.MessageExtractor(table.schema, MyProto)
my_proto_0 = message_extractor.read_table_row(table, 0)
my_proto_1 = message_extractor.read_table_row(table, 1)
```

## Customize arrow type

The arrow type for `Enum`, `Timestamp` and `TimeOfDay` and `Duration` can be configured:

```python
config = protarrow.ProtarrowConfig(
    enum_type=pa.int32(),
    timestamp_type=pa.timestamp("ms", "America/New_York"),
    time_of_day_type=pa.time32("ms"),
    duration_type=pa.duration("s"),
)
record_batch = protarrow.messages_to_record_batch(my_protos, MyProto, config)
```

## Cast existing table to proto schema

You can use this library to cast existing table to the expected proto schema. 

For example, if you have a table with missing columns:
```python
source_table = pa.table({"name": ["hello"]})
casted_table = protarrow.cast_table(source_table, MyProto, config)
```

This will fill the missing columns with default, or `None` when supported:

| name   |   id | values   |
|:-------|-----:|:---------|
| hello  |    0 | []       |
