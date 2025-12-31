"""
Tests the behavior of Google protobuf
"""

from google.protobuf.empty_pb2 import Empty
from google.protobuf.wrappers_pb2 import StringValue

from protarrow import messages_to_record_batch, record_batch_to_messages
from protarrow_protos.bench_pb2 import ExampleMessage, NestedExampleMessage
from protarrow_protos.example_pb2 import EmptyMessage, MessageWithOptional


def test_empty_has_field():
    message = EmptyMessage()
    assert not message.HasField("empty_value")
    message.empty_value.MergeFrom(Empty())
    assert message.HasField("empty_value")


def test_repeated_no_presence():
    field = MessageWithOptional.DESCRIPTOR.fields_by_name["string_values"]
    assert field.is_repeated
    assert not field.has_presence


def test_presence():
    default_value = MessageWithOptional()
    optional_present = MessageWithOptional(
        optional_string="", plain_string="", string_value=StringValue(value="")
    )
    optional_missing = MessageWithOptional(
        optional_string=None, plain_string="", string_value=None
    )

    assert not default_value.HasField("optional_string")
    assert not optional_missing.HasField("optional_string")
    assert optional_present.HasField("optional_string")
    assert optional_present != optional_missing

    messages = [default_value, optional_present, optional_missing]

    payload = optional_missing.SerializeToString()
    from_payload = MessageWithOptional()
    from_payload.FromString(payload)

    assert not from_payload.HasField("optional_string")
    assert from_payload == optional_missing
    assert from_payload != optional_present
    assert optional_missing.optional_string == ""

    record_batch = messages_to_record_batch(messages, MessageWithOptional)
    assert record_batch["optional_string"].to_pylist() == [None, "", None]
    assert record_batch["plain_string"].to_pylist() == ["", "", ""]
    assert record_batch["string_value"].to_pylist() == [None, "", None]

    assert record_batch.field("optional_string").nullable
    assert not record_batch.field("plain_string").nullable
    assert record_batch.field("string_value").nullable

    message_back = record_batch_to_messages(record_batch, MessageWithOptional)
    assert message_back == messages


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


def test_optional_enum():
    default_value = ExampleMessage()
    optional_present = ExampleMessage(optional_example_enum_value=1)
    optional_default = ExampleMessage(optional_example_enum_value=0)
    optional_missing = ExampleMessage(optional_example_enum_value=None)

    messages = [default_value, optional_present, optional_default, optional_missing]

    record_batch = messages_to_record_batch(messages, ExampleMessage)
    assert record_batch["optional_example_enum_value"].to_pylist() == [None, 1, 0, None]
    assert record_batch["example_enum_value"].to_pylist() == [0, 0, 0, 0]
