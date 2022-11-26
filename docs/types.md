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

* Top level native field, list and maps are marked as non-nullable.
* Any nested message and their children are nullable
