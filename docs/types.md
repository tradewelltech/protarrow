# Type Mapping

## Native Types

| Proto    | Pyarrow                                           | Note         |
|----------|---------------------------------------------------|--------------|
| bool     | bool_                                             |              |
| bytes    | **binary**/large_binary                           | Configurable |
| double   | float64                                           |              |
| enum     | **int32**/string/binary/large_string/large_binary | Configurable |
| fixed32  | int32                                             |              |
| fixed64  | int64                                             |              |
| float    | float32                                           |              |
| int32    | int32                                             |              |
| int64    | int64                                             |              |
| message  | struct                                            |              |
| sfixed32 | int32                                             |              |
| sfixed64 | int64                                             |              |
| sint32   | int32                                             |              |
| sint64   | int64                                             |              |
| string   | **string**/large_string                           | Configurable |
| uint32   | uint32                                            |              |
| uint64   | uint64                                            |              |

```python
protarrow.ProtarrowConfig(
    string_type=pa.large_string(),
    binary_type=pa.large_binary(),
    enum_type=pa.large_string(),
)
```

## Other types

| Proto                       | Pyarrow                 | Note                               |
|-----------------------------|-------------------------|------------------------------------|
| repeated                    | **list_**/large_list    | Configurable                       |
| map                         | map_                    |                                    |
| google.protobuf.BoolValue   | bool_                   |                                    |
| google.protobuf.BytesValue  | **binary**/large_binary | Configurable                       |
| google.protobuf.DoubleValue | float64                 |                                    |
| google.protobuf.Empty       | struct([])              |                                    |
| google.protobuf.Int32Value  | int32                   |                                    |
| google.protobuf.Int64Value  | int64                   |                                    |
| google.protobuf.StringValue | **string**/large_string | Configurable                       |
| google.protobuf.Timestamp   | timestamp("ns", "UTC")  | Unit and timezone are configurable |
| google.protobuf.UInt32Value | uint32                  |                                    |
| google.protobuf.UInt64Value | uint64                  |                                    |
| google.type.Date            | date32()                |                                    |
| google.type.TimeOfDay       | **time64**/time32       | Unit and type are configurable     |
| google.type.Duration        | duration("ns")          | Unit is configurable               |

```python
protarrow.ProtarrowConfig(
    list_array_type=pa.LargeListArray,
    timestamp_type=pa.timestamp("s", "UTC"),
    time_of_day_type=pa.time32("s"),
    duration_type=pa.duration("s"),
)
```

## Nullability

By default, nullability follows the convention imposed by protobuf:

- Primitive field, list, map, list value, map key and map value are non-nullable.
- Non-repeated messages and `optional` are the only nullable fields.

Some of this can be configured:

```python
protarrow.ProtarrowConfig(
    list_nullable=True,
    map_nullable=True,
    list_value_nullable=True,
    map_value_nullable=True,
)
```

## Map/List values fields names

You can also customize the name of the `pa.list_` and `pa.map_` items names.
This doesn't semantically change the schema of the table, but may change its string representation.

```python
protarrow.ProtarrowConfig(
    list_value_name="array",
    map_value_name="map_value",
)
```

For example this will change a `repated int32` field's arrow type from `ListType(list<item: int32>)` to `ListType(list<array: int32>)`.
