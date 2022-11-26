
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
record_batch = protarrow.messages_to_record_batch(my_protos, MyProto)
table = protarrow.messages_to_table(my_protos, MyProto)
```


| name   |   id | values   |
|:-------|-----:|:---------|
| foo    |    1 | [1 2 4]  |
| bar    |    2 | [3 4 5]  |


## Convert from arrow to proto

```python
protos_from_record_batch = protarrow.table_to_messages(record_batch, MyProto)
protos_from_table = protarrow.table_to_messages(table, MyProto)
```

## Customize arrow type

The arrow type for `Enum`, `Timestamp` and `TimeOfDay` can be configured:

```python
config = protarrow.ProtarrowConfig(
    enum_type=pa.int32(),
    timestamp_type=pa.timestamp("ms", "America/New_York"),
    time_of_day_type=pa.time32("ms"),
)
record_batch = protarrow.messages_to_record_batch(my_protos, MyProto, config)
```
