"""
Tests the behavior of Google protobuf
"""

from google.protobuf.empty_pb2 import Empty

from protarrow_protos.example_pb2 import EmptyMessage


def test_empty_has_field():
    message = EmptyMessage()
    assert not message.HasField("empty_value")
    message.empty_value.MergeFrom(Empty())
    assert message.HasField("empty_value")
