from protarrow_protos.example_pb2 import MessageWithOptional
from tests.random_generator import generate_messages


def test_with_optional():
    """Make sure optional fields are sometime missing"""
    messages = generate_messages(MessageWithOptional, 100)
    assert any(m.HasField("optional_string") for m in messages)
    assert any(not m.HasField("optional_string") for m in messages)

    assert any(m.HasField("string_value") for m in messages)
    assert any(not m.HasField("string_value") for m in messages)
