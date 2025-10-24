import pathlib
from typing import List, Type

import pyarrow as pa
import pytest
from google.protobuf.message import Message

from protarrow.common import ProtarrowConfig
from protarrow.proto_to_arrow import (
    message_type_to_schema,
    message_type_to_struct_type,
    messages_to_record_batch,
    messages_to_table,
)
from protarrow_protos.bench_pb2 import (
    RecursiveNestedMessageLevel1,
    RecursiveSelfReferentialMapMessage,
    RecursiveSelfReferentialMessage,
    RecursiveSelfReferentialRepeatedMessage,
)

from .test_conversion import read_proto_jsonl

CONFIGS = [
    ProtarrowConfig(skip_recursive_messages=False),
    ProtarrowConfig(skip_recursive_messages=True),
]
DIR = pathlib.Path(__file__).parent


def _load_data(filename: str, message_type: Type[Message]) -> List[Message]:
    """Loads messages from the specific test data file."""
    source_file = DIR / "data" / filename
    source_messages = read_proto_jsonl(source_file, message_type)
    if not source_messages:
        raise ValueError(f"Found empty test file: {source_file}")
    return source_messages


# ====================================================================
# RECURSIVE SELF-REFERENTIAL MESSAGES:
#  mes A:         mes A:
#    mes A:  =>     (ES)
#
# (ES): empty struct
# ====================================================================
@pytest.mark.parametrize("config", CONFIGS)
def test_recursive_self_referential_message_handling(config: ProtarrowConfig):
    messages = _load_data(
        "RecursiveSelfReferentialMessage.jsonl", RecursiveSelfReferentialMessage
    )

    if not config.skip_recursive_messages:
        fqn = "protarrow.protos.RecursiveSelfReferentialMessage".replace(".", r"\.")
        regex_pattern = r"(.*" + f"{fqn}, {fqn}" + r".*)"

        with pytest.raises(TypeError, match=regex_pattern):
            messages_to_record_batch(messages, RecursiveSelfReferentialMessage, config)

        with pytest.raises(TypeError, match=regex_pattern):
            messages_to_table(messages, RecursiveSelfReferentialMessage, config)

        with pytest.raises(TypeError, match=regex_pattern):
            message_type_to_schema(RecursiveSelfReferentialMessage, config)

        with pytest.raises(TypeError, match=regex_pattern):
            message_type_to_struct_type(RecursiveSelfReferentialMessage, config)

    else:
        rb = messages_to_record_batch(messages, RecursiveSelfReferentialMessage, config)
        inferred_schema = message_type_to_schema(
            RecursiveSelfReferentialMessage, config
        )
        inferred_type = message_type_to_struct_type(
            RecursiveSelfReferentialMessage, config
        )

        # Check schema
        expected_schema = pa.schema(
            [
                pa.field("next", pa.struct([])),
                pa.field("depth", pa.int32(), nullable=False),
            ]
        )
        expected_type = pa.struct(expected_schema)

        assert rb.schema == expected_schema
        assert inferred_schema == expected_schema
        assert inferred_type == expected_type

        # Check values
        expected_depth_array = pa.array([1, 11, 21], type=pa.int32())
        expected_next_array = pa.StructArray.from_arrays(
            arrays=[],
            fields=[],
            mask=pa.array([False] * len(expected_depth_array), pa.bool_()),
        )
        expected_table = pa.Table.from_arrays(
            [expected_next_array, expected_depth_array], schema=expected_schema
        )
        expected_table = pa.Table.from_arrays(
            [expected_next_array, expected_depth_array], schema=expected_schema
        )

        actual_table = pa.Table.from_batches([rb])
        assert actual_table.equals(expected_table)


# ====================================================================
# NESTED RECURSIVE MESSAGES:
# mes A:       mes A:
#   mes B:  =>   mes B:
#     mes A:       (ES)
#
# (ES): empty struct
# ====================================================================
@pytest.mark.parametrize("config", CONFIGS)
def test_recursive_nested_message_handling(config: ProtarrowConfig):
    messages = _load_data(
        "RecursiveNestedMessageLevel1.jsonl", RecursiveNestedMessageLevel1
    )

    if not config.skip_recursive_messages:
        fqn1 = "protarrow.protos.RecursiveNestedMessageLevel1".replace(".", r"\.")
        fqn2 = "protarrow.protos.RecursiveNestedMessageLevel2".replace(".", r"\.")
        fqn3 = "protarrow.protos.RecursiveNestedMessageLevel3".replace(".", r"\.")
        expected_trace_string = f"{fqn1}, {fqn2}, {fqn3}, {fqn1}"
        regex_pattern = r"(.*" + f"{expected_trace_string}" + r".*)"

        with pytest.raises(TypeError, match=regex_pattern):
            messages_to_record_batch(messages, RecursiveNestedMessageLevel1, config)

        with pytest.raises(TypeError, match=regex_pattern):
            messages_to_table(messages, RecursiveNestedMessageLevel1, config)

        with pytest.raises(TypeError, match=regex_pattern):
            message_type_to_schema(RecursiveNestedMessageLevel1, config)

        with pytest.raises(TypeError, match=regex_pattern):
            message_type_to_struct_type(RecursiveNestedMessageLevel1, config)

    else:
        rb = messages_to_record_batch(messages, RecursiveNestedMessageLevel1, config)
        inferred_schema = message_type_to_schema(RecursiveNestedMessageLevel1, config)
        inferred_type = message_type_to_struct_type(
            RecursiveNestedMessageLevel1, config
        )

        # Check schema
        pruned_struct = pa.struct([])
        level3_struct = pa.struct(
            [
                pa.field("name", pa.string(), nullable=False),
                pa.field("next", pruned_struct),
            ]
        )
        level2_struct = pa.struct(
            [
                pa.field("name", pa.string(), nullable=False),
                pa.field("next", level3_struct),
            ]
        )
        expected_schema = pa.schema(
            [
                pa.field("name", pa.string(), nullable=False),
                pa.field("next", level2_struct),
            ]
        )
        expected_type = pa.struct(expected_schema)

        assert rb.schema == expected_schema
        assert inferred_schema == expected_schema
        assert inferred_type == expected_type

        # Check values
        num_rows = 3

        level3_name_array = pa.array(
            [f"M{i}_L3" for i in range(1, num_rows + 1)], pa.string()
        )
        level3_pruned_array = pa.StructArray.from_arrays(
            arrays=[],
            fields=[],
            mask=pa.array([False] * len(level3_name_array), pa.bool_()),
        )
        level3_array = pa.StructArray.from_arrays(
            arrays=[level3_name_array, level3_pruned_array],
            fields=[level3_struct.field("name"), level3_struct.field("next")],
        )

        level2_name_array = pa.array(
            [f"M{i}_L2" for i in range(1, num_rows + 1)], pa.string()
        )
        level2_array = pa.StructArray.from_arrays(
            arrays=[level2_name_array, level3_array],
            fields=[level2_struct.field("name"), level2_struct.field("next")],
        )

        level1_name_array = pa.array(
            [f"M{i}_L1" for i in range(1, num_rows + 1)], pa.string()
        )
        expected_table = pa.Table.from_arrays(
            [level1_name_array, level2_array],
            schema=expected_schema,
        )

        actual_table = pa.Table.from_batches([rb])
        assert actual_table.equals(expected_table)


# ====================================================================
# RECURSIVE SELF-REFERENTIAL REPEATED MESSAGES
# mes A:                      mes A:
#   [mes A, mes A, mes A]  =>   [(ES), (ES), (ES)]
#
# (ES): empty struct
# ====================================================================
@pytest.mark.parametrize("config", CONFIGS)
def test_recursive_self_referential_repeated_message_handling(config: ProtarrowConfig):
    messages = _load_data(
        "RecursiveSelfReferentialRepeatedMessage.jsonl",
        RecursiveSelfReferentialRepeatedMessage,
    )

    fqn = "protarrow.protos.RecursiveSelfReferentialRepeatedMessage".replace(".", r"\.")
    # The expected trace is the recursive field name repeated twice
    regex_pattern = r".*" + f"({fqn}, {fqn})" + r".*"

    if not config.skip_recursive_messages:
        with pytest.raises(TypeError, match=regex_pattern):
            messages_to_record_batch(
                messages, RecursiveSelfReferentialRepeatedMessage, config
            )
        with pytest.raises(TypeError, match=regex_pattern):
            messages_to_table(messages, RecursiveSelfReferentialRepeatedMessage, config)

        with pytest.raises(TypeError, match=regex_pattern):
            message_type_to_schema(RecursiveSelfReferentialRepeatedMessage, config)

        with pytest.raises(TypeError, match=regex_pattern):
            message_type_to_struct_type(RecursiveSelfReferentialRepeatedMessage, config)

    else:
        rb = messages_to_record_batch(
            messages, RecursiveSelfReferentialRepeatedMessage, config
        )
        inferred_schema = message_type_to_schema(
            RecursiveSelfReferentialRepeatedMessage, config
        )
        inferred_type = message_type_to_struct_type(
            RecursiveSelfReferentialRepeatedMessage, config
        )

        # Check schema
        pruned_item_field = pa.field(
            name=config.list_value_name,
            type=pa.struct([]),
            nullable=config.list_value_nullable,
        )
        expected_children_list_type = pa.list_(pruned_item_field)

        expected_schema = pa.schema(
            [
                pa.field("depth", pa.int32(), nullable=False),
                pa.field(
                    "children",
                    expected_children_list_type,
                    nullable=config.list_value_nullable,
                ),
            ]
        )
        expected_type = pa.struct(expected_schema)

        assert rb.schema == expected_schema
        assert inferred_schema == expected_schema
        assert inferred_type == expected_type

        # Check values
        expected_depth_array = pa.array([1, 11, 21], pa.int32())
        child_struct_array = pa.StructArray.from_arrays(
            arrays=[],
            fields=[],
            mask=pa.array([False] * len(expected_depth_array), pa.bool_()),
        )

        list_offsets = pa.array([0, 1, 3, 3], pa.int32())
        expected_children_list_array = pa.ListArray.from_arrays(
            offsets=list_offsets,
            values=child_struct_array,
            type=expected_children_list_type,
        )

        expected_table = pa.Table.from_arrays(
            [expected_depth_array, expected_children_list_array], schema=expected_schema
        )
        actual_table = pa.Table.from_batches([rb])

        assert actual_table.equals(expected_table)


# ====================================================================
# RECURSIVE SELF-REFERENTIAL MAP MESSAGES
#  mes A:                mes A:
#    map<*, mes A>  =>     map<*, (ES)>
# ====================================================================
@pytest.mark.parametrize("config", CONFIGS)
def test_recursive_self_referential_map_message_handling(config: ProtarrowConfig):
    messages = _load_data(
        "RecursiveSelfReferentialMapMessage.jsonl", RecursiveSelfReferentialMapMessage
    )

    fqn = "protarrow.protos.RecursiveSelfReferentialMapMessage".replace(".", r"\.")
    regex_pattern = r".*" + f"({fqn}, {fqn})" + r".*"

    if not config.skip_recursive_messages:
        with pytest.raises(TypeError, match=regex_pattern):
            messages_to_record_batch(
                messages, RecursiveSelfReferentialMapMessage, config
            )

        with pytest.raises(TypeError, match=regex_pattern):
            messages_to_table(messages, RecursiveSelfReferentialMapMessage, config)

        with pytest.raises(TypeError, match=regex_pattern):
            message_type_to_schema(RecursiveSelfReferentialMapMessage, config)

        with pytest.raises(TypeError, match=regex_pattern):
            message_type_to_struct_type(RecursiveSelfReferentialMapMessage, config)

    else:
        rb = messages_to_record_batch(
            messages, RecursiveSelfReferentialMapMessage, config
        )
        inferred_schema = message_type_to_schema(
            RecursiveSelfReferentialMapMessage, config
        )
        inferred_type = message_type_to_struct_type(
            RecursiveSelfReferentialMapMessage, config
        )

        # Check schema
        pruned_value_struct = pa.struct([])
        key_type = pa.string()
        value_field = pa.field(
            name=config.map_value_name,
            type=pruned_value_struct,
            nullable=config.map_value_nullable,
        )
        children_map_type = pa.map_(key_type, value_field)

        expected_schema = pa.schema(
            [
                pa.field("name", pa.string(), nullable=False),
                pa.field("children_map", children_map_type, nullable=False),
            ]
        )
        expected_type = pa.struct(expected_schema)

        assert rb.schema == expected_schema, (
            "Schema mismatch for map self-reference pruning."
        )
        assert inferred_schema == expected_schema
        assert inferred_type == expected_type

        # Check values
        key_array = pa.array(["A", "D", "E"], pa.string())
        pruned_value_array = pa.StructArray.from_arrays(
            arrays=[], fields=[], mask=pa.array([False] * len(key_array))
        )
        expected_name_array = pa.array(["M1_L1", "M2_L1", "M3_L1"], pa.string())
        # Offsets for 1 item (M1), 2 items (M2), 0 items (M3)
        list_offsets = pa.array([0, 1, 3, 3], pa.int32())

        expected_children_map_array = pa.MapArray.from_arrays(
            offsets=list_offsets,
            keys=key_array,
            items=pruned_value_array,
            type=children_map_type,
        )
        expected_table = pa.Table.from_arrays(
            [expected_name_array, expected_children_map_array], schema=expected_schema
        )

        actual_table = pa.Table.from_batches([rb])
        assert actual_table.equals(expected_table)
