"""
Tests the behavior of Google protobuf
"""

from google.protobuf.empty_pb2 import Empty

from protarrow import messages_to_record_batch
from protarrow_protos.bench_pb2 import ExampleMessage, NestedExampleMessage
from protarrow_protos.example_pb2 import EmptyMessage, MessageWithOptional


def test_empty_has_field():
    message = EmptyMessage()
    assert not message.HasField("empty_value")
    message.empty_value.MergeFrom(Empty())
    assert message.HasField("empty_value")


def test_presence():
    messages = [
        MessageWithOptional(),
        MessageWithOptional(optional_string="HELLO", plain_string=""),
        MessageWithOptional(optional_string=None, plain_string=""),
    ]

    optional_present = MessageWithOptional(optional_string="", plain_string="")
    optional_missing = MessageWithOptional(optional_string=None, plain_string="")
    assert not optional_missing.HasField("optional_string")
    assert optional_present.HasField("optional_string")

    assert optional_present != optional_missing

    payload = optional_missing.SerializeToString()
    from_payload = MessageWithOptional()
    from_payload.FromString(payload)

    assert not from_payload.HasField("optional_string")
    assert from_payload == optional_missing
    assert from_payload != optional_present
    assert optional_missing.optional_string == ""

    results = messages_to_record_batch(
        messages,
        MessageWithOptional,
    )


def test_presence_example_message():
    descriptor = ExampleMessage.DESCRIPTOR
    assert not descriptor.fields_by_name["double_value"].has_presence
    assert descriptor.fields_by_name["wrapped_double_value"].has_presence
    assert not descriptor.fields_by_name["double_values"].has_presence
    assert not descriptor.fields_by_name["wrapped_double_values"].has_presence
    assert not descriptor.fields_by_name["double_int32_map"].has_presence


def test_presence_nested_example_message():
    descriptor = NestedExampleMessage.DESCRIPTOR
    assert descriptor.fields_by_name["example_message"].has_presence
    assert not descriptor.fields_by_name["repeated_example_message"].has_presence
    assert not descriptor.fields_by_name["example_message_int32_map"].has_presence
    assert not descriptor.fields_by_name["example_message_string_map"].has_presence
