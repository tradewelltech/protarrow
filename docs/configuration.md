# Configuration

`ProtarrowConfig` customizes the arrow representation produced when converting
from protobuf. It is accepted by the proto→arrow functions
(`messages_to_record_batch`, `messages_to_table`, `message_type_to_schema`,
`message_type_to_struct_type`) and by the cast functions (`cast_record_batch`,
`cast_struct_array`, `cast_table`). The arrow→proto direction
(`record_batch_to_messages`, `table_to_messages`, `MessageExtractor`) reads the
arrow schema directly and does not take a config.

```python
import protarrow
import pyarrow as pa

config = protarrow.ProtarrowConfig(
    timestamp_type=pa.timestamp("ms", "America/New_York"),
    enum_type=pa.dictionary(pa.int32(), pa.string()),
)

table = protarrow.messages_to_table(my_protos, MyProto, config)
```

The sections below describe every option, grouped by purpose.

## Type customization

These options control which arrow types are used for proto types that have
multiple valid representations.

### `enum_type`

Type used for protobuf `enum` fields.

- **Default:** `pa.int32()`
- **Accepted:** `pa.int32()`, `pa.string()`, `pa.large_string()`, `pa.binary()`,
  `pa.large_binary()`, `pa.dictionary(pa.int32(), pa.string())`,
  `pa.dictionary(pa.int32(), pa.binary())`

When using a string or binary `enum_type`, it must match `string_type` or
`binary_type` respectively. Dictionary-encoded enums with `large_string` or
`large_binary` values are not supported (PyArrow limitation).

```python
protarrow.ProtarrowConfig(enum_type=pa.dictionary(pa.int32(), pa.string()))
```

### `string_type`

Type used for protobuf `string` fields and `google.protobuf.StringValue`.

- **Default:** `pa.string()`
- **Accepted:** `pa.string()`, `pa.large_string()`

### `binary_type`

Type used for protobuf `bytes` fields and `google.protobuf.BytesValue`.

- **Default:** `pa.binary()`
- **Accepted:** `pa.binary()`, `pa.large_binary()`

### `timestamp_type`

Type used for `google.protobuf.Timestamp`.

- **Default:** `pa.timestamp("ns", "UTC")`
- **Accepted:** any `pa.timestamp(unit, tz)`

### `time_of_day_type`

Type used for `google.type.TimeOfDay`.

- **Default:** `pa.time64("ns")`
- **Accepted:** any `pa.time32(unit)` or `pa.time64(unit)`

### `duration_type`

Type used for `google.type.Duration`.

- **Default:** `pa.duration("ns")`
- **Accepted:** any `pa.duration(unit)`

### `list_array_type`

Array class used for protobuf `repeated` fields. Selects between regular and
large list encoding.

- **Default:** `pa.ListArray`
- **Accepted:** `pa.ListArray`, `pa.LargeListArray`

```python
protarrow.ProtarrowConfig(list_array_type=pa.LargeListArray)
```

## Nullability

By default, nullability follows protobuf semantics: primitive fields, lists,
maps, list values, map keys and map values are all non-nullable; only
non-repeated messages and `optional` fields are nullable. The flags below relax
that for the container fields.

### `list_nullable`

Whether the `repeated` field itself can be null.

- **Default:** `False`

### `map_nullable`

Whether the `map` field itself can be null.

- **Default:** `False`

### `list_value_nullable`

Whether the values inside a `repeated` field can be null.

- **Default:** `False`

### `map_value_nullable`

Whether the values inside a `map` field can be null.

- **Default:** `False`

```python
protarrow.ProtarrowConfig(
    list_nullable=True,
    map_nullable=True,
    list_value_nullable=True,
    map_value_nullable=True,
)
```

## Field naming

These options change the names of the inner fields in `list_` and `map_` types.
They do not change the data, only the schema's string representation.

### `list_value_name`

Name of the value field inside a list type.

- **Default:** `"item"`

For example, this changes a `repeated int32` field's arrow type from
`list<item: int32>` to `list<array: int32>`:

```python
protarrow.ProtarrowConfig(list_value_name="array")
```

### `map_value_name`

Name of the value field inside a map type.

- **Default:** `"value"`

## Advanced

### `field_number_key`

If set, the proto field number is written into each arrow field's metadata
under this key. Useful for round-tripping through formats that preserve field
metadata (e.g. parquet).

- **Default:** `None` (no metadata is added)
- **Accepted:** `bytes` or `None`

```python
config = protarrow.ProtarrowConfig(field_number_key=b"PARQUET:field_id")
schema = protarrow.message_type_to_schema(MyProto, config)
# schema fields now carry {b"PARQUET:field_id": b"<field_number>"}
```

### `skip_recursive_messages`

By default, converting a protobuf with a recursive message structure raises a
`TypeError`. When set to `True`, recursive sub-messages are emitted as an empty
`pa.struct([])` instead, breaking the recursion.

- **Default:** `False`

```python
protarrow.ProtarrowConfig(skip_recursive_messages=True)
```

### `map_as_list`

When `True`, protobuf `map` fields are converted to a list of structs (with
`key` and value fields) rather than `pa.map_`. This is useful for tools that
do not handle arrow's map type well, and supports bidirectional conversion.

- **Default:** `False`

```python
protarrow.ProtarrowConfig(map_as_list=True)
```

The resulting arrow type for `map<string, int32>` becomes
`list<struct<key: string, value: int32>>` instead of
`map<string, int32>`. The inner field names follow `map_value_name` and the
list encoding follows `list_array_type`.

This was introduced for system like BigQuery who converts map to list of key/value struct.
