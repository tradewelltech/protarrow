from typing import Any, Optional, Tuple, Type

import pyarrow as pa
import pyarrow.compute as pc
from google.protobuf.descriptor import Descriptor, FieldDescriptor
from google.protobuf.message import Message
from google.protobuf.timestamp_pb2 import Timestamp
from google.type.timeofday_pb2 import TimeOfDay

from protarrow.arrow_to_proto import is_binary_enum, is_string_enum
from protarrow.common import ProtarrowConfig
from protarrow.proto_to_arrow import (
    _PROTO_DESCRIPTOR_TO_PYARROW,
    _PROTO_PRIMITIVE_TYPE_TO_PYARROW,
    field_descriptor_to_field,
    get_map_descriptors,
    is_map,
    message_type_to_schema,
)


def get_arrow_default_value(
    field_descriptor: FieldDescriptor, config: ProtarrowConfig
) -> Any:
    if field_descriptor.type == FieldDescriptor.TYPE_ENUM:
        default_value = (
            field_descriptor.enum_type.values[0].number
            if field_descriptor.label == FieldDescriptor.LABEL_REPEATED
            else field_descriptor.default_value
        )
        if pa.types.is_integer(config.enum_type):
            return default_value
        elif is_string_enum(config.enum_type):
            return field_descriptor.enum_type.values[default_value].name
        elif is_binary_enum(config.enum_type):
            return field_descriptor.enum_type.values[default_value].name.encode("utf-8")
        else:
            raise TypeError(config.enum_type)
    else:
        return field_descriptor.default_value


def _cast_flat_array(
    array: pa.Array,
    field_descriptor: FieldDescriptor,
    config: ProtarrowConfig,
) -> pa.Array:
    assert not pa.types.is_list(array.type), field_descriptor.name
    assert not pa.types.is_map(array.type), field_descriptor.name
    if field_descriptor.type == FieldDescriptor.TYPE_MESSAGE:
        if field_descriptor.message_type == TimeOfDay.DESCRIPTOR:
            return array.cast(config.time_of_day_type)
        elif field_descriptor.message_type == Timestamp.DESCRIPTOR:
            return array.cast(config.timestamp_type)
        elif field_descriptor.message_type in _PROTO_DESCRIPTOR_TO_PYARROW:
            return array.cast(
                _PROTO_DESCRIPTOR_TO_PYARROW[field_descriptor.message_type]
            )
        else:
            assert isinstance(array, pa.StructArray), field_descriptor.message_type
            return cast_struct_array(array, field_descriptor.message_type, config)
    else:
        if field_descriptor.type == FieldDescriptor.TYPE_ENUM:
            if pa.types.is_dictionary(config.enum_type) and not pa.types.is_dictionary(
                array.type
            ):
                results = pc.dictionary_encode(array.cast(config.enum_type.value_type))
                assert results.type == config.enum_type
            else:
                results = array.cast(config.enum_type)
        else:
            results = array.cast(
                _PROTO_PRIMITIVE_TYPE_TO_PYARROW[field_descriptor.type]
            )
        if results.null_count > 0:
            return results.fill_null(get_arrow_default_value(field_descriptor, config))
        else:
            return results


def _cast_array(
    array: pa.Array,
    field_descriptor: FieldDescriptor,
    config: ProtarrowConfig,
) -> pa.Array:
    if is_map(field_descriptor):
        assert isinstance(array, pa.MapArray)
        key_field, value_field = get_map_descriptors(field_descriptor)
        map_array = pa.MapArray.from_arrays(
            array.offsets,
            _cast_array(array.keys, key_field, config),
            _cast_array(array.items, value_field, config),
        )
        return map_array.cast(
            pa.map_(
                map_array.type.key_type,
                pa.field(
                    config.map_value_name,
                    map_array.type.item_type,
                    nullable=config.map_value_nullable,
                ),
            )
        )

    elif field_descriptor.label == FieldDescriptor.LABEL_REPEATED:
        assert isinstance(array, pa.ListArray)
        item_array = _cast_flat_array(array.values, field_descriptor, config)
        return pa.ListArray.from_arrays(
            array.offsets,
            item_array,
            pa.list_(
                pa.field(
                    config.list_value_name,
                    item_array.type,
                    nullable=config.list_value_nullable,
                )
            ),
        )
    else:
        return _cast_flat_array(array, field_descriptor, config)


def get_casted_array(
    field_descriptor: FieldDescriptor,
    source_array: Optional[pa.Array],
    num_rows: int,
    config: ProtarrowConfig,
) -> Tuple[pa.Array, pa.Field]:
    expected_field = field_descriptor_to_field(field_descriptor, config)
    if source_array is not None:
        casted_array = _cast_array(source_array, field_descriptor, config)
    elif expected_field.nullable:
        casted_array = pa.nulls(num_rows, expected_field.type)
        if pa.types.is_struct(expected_field.type):
            casted_array = cast_struct_array(
                casted_array, field_descriptor.message_type, config
            )
    else:
        default_value = (
            []
            if field_descriptor.label == FieldDescriptor.LABEL_REPEATED
            else get_arrow_default_value(field_descriptor, config)
        )
        casted_array = pa.array(
            [default_value] * num_rows,
            size=num_rows,
            type=expected_field.type,
        )

    return casted_array, expected_field


def cast_record_batch(
    record_batch: pa.RecordBatch,
    message_type: Type[Message],
    config: ProtarrowConfig,
) -> pa.RecordBatch:
    arrays = []
    fields = []
    for field_descriptor in message_type.DESCRIPTOR.fields:
        field_index = record_batch.schema.get_field_index(field_descriptor.name)
        array, field = get_casted_array(
            field_descriptor,
            record_batch.column(field_index) if field_index >= 0 else None,
            record_batch.num_rows,
            config,
        )
        arrays.append(array)
        fields.append(field)
    return pa.RecordBatch.from_arrays(arrays=arrays, schema=pa.schema(fields))


def cast_struct_array(
    struct_array: pa.StructArray,
    descriptor: Descriptor,
    config: ProtarrowConfig,
) -> pa.StructArray:
    arrays = []
    fields = []
    for field_descriptor in descriptor.fields:
        field_index = struct_array.type.get_field_index(field_descriptor.name)
        array, field = get_casted_array(
            field_descriptor,
            struct_array.field(field_index) if field_index >= 0 else None,
            len(struct_array),
            config,
        )
        arrays.append(array)
        fields.append(field)
    if len(arrays) == 0:
        # TODO: remove when this is fixed
        #  https://github.com/apache/arrow/issues/15109
        return pa.StructArray.from_arrays(
            arrays=[pa.nulls(len(struct_array))],
            fields=[pa.field("null", pa.null())],
            mask=struct_array.is_null(),
        ).cast(pa.struct([]))
    else:
        return pa.StructArray.from_arrays(
            arrays=arrays, fields=fields, mask=struct_array.is_null()
        )


def cast_table(
    table: pa.Table, message_type: Type[Message], config: ProtarrowConfig
) -> pa.Table:
    proto_schema = message_type_to_schema(message_type, config)
    record_batches = []
    for record_batch in table.to_reader():
        record_batches.append(cast_record_batch(record_batch, message_type, config))
    return pa.Table.from_batches(record_batches, proto_schema)
