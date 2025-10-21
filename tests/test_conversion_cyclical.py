# Imports sorted alphabetically
import pathlib
import pytest

# 'from' imports, sorted alphabetically by module
from google.protobuf.json_format import Parse
from google.protobuf.message import Message
from protarrow.common import M, ProtarrowConfig
from protarrow.proto_to_arrow import (
    messages_to_record_batch,
    ProtarrowCycleError,
    messages_to_table,
)
from protarrow_protos.bench_pb2 import (
    CyclicalDirectMessage,
    CyclicalIndirectMessageLevel1,
    CyclicalMapMessage,
    CyclicalRepeatedMessage,
)
from typing import List, Type

CONFIGS = [
    ProtarrowConfig(purge_cyclical_messages=False),
    ProtarrowConfig(purge_cyclical_messages=True),
]
DIR = pathlib.Path(__file__).parent


def read_proto_jsonl(path: pathlib.Path, message_type: Type[M]) -> List[M]:
    with path.open() as fp:
        return [
            Parse(line.strip(), message_type())
            for line in fp
            if line.strip() and not line.startswith("#")
        ]


def _load_data(filename: str, message_type: Type[Message]) -> List[Message]:
    """Loads messages from the specific test data file."""
    source_file = DIR / "data" / filename
    source_messages = read_proto_jsonl(source_file, message_type)
    if not source_messages:
        pytest.skip(f"Test data file {filename} is empty or missing.")
    return source_messages


# ====================================================================
# DIRECT SELF-REFERENCE
#  X           X
#  A - Y  =>   A
#      A
# ====================================================================
@pytest.mark.parametrize("config", CONFIGS)
def test_cyclical_direct_message_handling(config: ProtarrowConfig):
    messages = _load_data("CyclicalDirectMessage.jsonl", CyclicalDirectMessage)

    if not config.purge_cyclical_messages:
        with pytest.raises(ProtarrowCycleError):
            messages_to_record_batch(messages, CyclicalDirectMessage, config)

        with pytest.raises(ProtarrowCycleError):
            messages_to_table(messages, CyclicalDirectMessage, config)

    else:
        rb = messages_to_record_batch(messages, CyclicalDirectMessage, config)
        assert len(rb) == len(messages)
        assert rb.num_columns == 2
        assert rb["next"].type.num_fields == 0


# ====================================================================
# INDIRECT CYCLE
#  L1                  L1
#  A - L2       =>     A - L2
#      B  - L3              B - L3
#            C - L4              C
#                 A
# ====================================================================
@pytest.mark.parametrize("config", CONFIGS)
def test_cyclical_indirect_message_handling(config: ProtarrowConfig):
    messages = _load_data(
        "CyclicalIndirectMessageLevel1.jsonl", CyclicalIndirectMessageLevel1
    )

    if not config.purge_cyclical_messages:
        with pytest.raises(ProtarrowCycleError):
            messages_to_record_batch(messages, CyclicalIndirectMessageLevel1, config)

        with pytest.raises(ProtarrowCycleError):
            messages_to_table(messages, CyclicalIndirectMessageLevel1, config)

    else:
        rb = messages_to_record_batch(messages, CyclicalIndirectMessageLevel1, config)
        assert len(rb) == len(messages)
        assert rb.num_columns == 2
        assert rb.schema.names == ["next", "name"]

        datadict = rb.to_pylist()[0]
        # Levels 1 to 3
        for i, level_name in enumerate(["L1", "L2", "L3"]):
            assert datadict["name"] == level_name
            datadict = datadict["next"]

        # Level 4 should have been pruned due to its type being
        assert not datadict


# ====================================================================
# CYCLICAL REPEATED MESSAGE
#  L1            L1
#  -             -
#  A             A
#  A - L2   =>   A
#  A   -         A
#  -   A         -
#      A
#      A
#      -
# ====================================================================
# We only support cycle detection and exception raising here
@pytest.mark.parametrize("config", CONFIGS)
def test_cyclical_repeated_message_handling(config: ProtarrowConfig):
    messages = _load_data("CyclicalRepeatedMessage.jsonl", CyclicalRepeatedMessage)

    with pytest.raises(ProtarrowCycleError):
        messages_to_record_batch(messages, CyclicalRepeatedMessage, config)

    with pytest.raises(ProtarrowCycleError):
        messages_to_table(messages, CyclicalRepeatedMessage, config)


# ====================================================================
# CYCLICAL MAP MESSAGE
#   L1 k1            L1 k1
#      |
#   {L2 k2}     =>
#         |
#      {L3 k3}
# ====================================================================
# We only support cycle detection and exception raising here
@pytest.mark.parametrize("config", CONFIGS)
def test_cyclical_map_message_handling(config: ProtarrowConfig):
    messages = _load_data("CyclicalMapMessage.jsonl", CyclicalMapMessage)

    with pytest.raises(ProtarrowCycleError):
        messages_to_record_batch(messages, CyclicalMapMessage, config)

    with pytest.raises(ProtarrowCycleError):
        messages_to_table(messages, CyclicalMapMessage, config)
