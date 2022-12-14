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
| google.type.TimeOfDay       | **time64**/time32      | Unit and type are configurable     |

## Nullability

By default, nullability follows the convention imposed by protobuf:
- Native field, list, map, list value, map key, map value are marked as non-nullable.
- Non-repeated messages are the only nullable fields. 


Some of this can be configured:
```python
import protarrow

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
import protarrow

protarrow.ProtarrowConfig(
    list_value_name="array",
    map_value_name="map_value",
)
```

For example this will change a `repated int32` field's arrow type from `ListType(list<item: int32>)` to `ListType(list<array: int32>)`. 
