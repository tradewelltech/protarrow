import io

from black.cache import dataclass
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.internal.type_checkers import TYPE_TO_ENCODER, TYPE_TO_DECODER
from google.protobuf.message import Message
from protarrow_protos.extension_pb2 import DESCRIPTOR, Base, Nested


def encode_extensions_simple(message: Message) -> bytes:
    copy = message.__class__()
    for extension_descriptor in message.Extensions:
        if extension_descriptor.type == FieldDescriptor.TYPE_MESSAGE:
            copy.Extensions[extension_descriptor].MergeFrom(
                message.Extensions[extension_descriptor]
            )
        else:
            copy.Extensions[extension_descriptor] = message.Extensions[
                extension_descriptor
            ]
    return copy.SerializeToString()


def decode_extensions_simple(payload: bytes, message: Message) -> Message:
    message.MergeFromString(payload)
    return message


def run_round_trip(message: Message):
    payload = encode_extensions_simple(message)
    result = decode_extensions_simple(payload, message.__class__())
    assert result == message
    return result


@dataclass(frozen=True)
class _Wrapper:
    message: Message

    def ByteSize(self):
        return self.message.ByteSize()

    def _InternalSerialize(self, writer, deterministic):
        writer(self.message.SerializeToString(deterministic=deterministic))


def encode_extension(message: Message, extension: FieldDescriptor) -> bytes:
    repeated = extension.label == FieldDescriptor.LABEL_REPEATED
    value = message.Extensions[extension]

    if extension.type == FieldDescriptor.TYPE_MESSAGE:
        if repeated:
            value = [_Wrapper(m) for m in value]
        else:
            value = _Wrapper(value)

    encoder = TYPE_TO_ENCODER[extension.type](
        field_number=extension.number,
        is_repeated=extension.label == FieldDescriptor.LABEL_REPEATED,
        is_packed=extension.is_packed,
    )

    with io.BytesIO() as buffer:
        encoder(write=buffer.write, value=value, deterministic=True)
        # extension._encoder(write=buffer.write, value=value, deterministic=True)
        buffer.seek(0)
        return buffer.read()


def encode_extensions(message: Message) -> dict[int, bytes]:
    return {
        extension.number: encode_extension(message, extension)
        for extension in message.Extensions
    }


def decode_extensions(message: Message, payloads: dict[int, bytes]) -> Message:
    if payloads:
        full_payload = b"".join(payloads.values())
        message.MergeFromString(full_payload)
    return message


def decode_extension(payload, extension_descriptor: FieldDescriptor):
    decoder = TYPE_TO_DECODER[extension_descriptor.type](
        is_repeated=extension_descriptor.label == FieldDescriptor.LABEL_REPEATED,
        is_packed=extension_descriptor.is_packed,
        field_number=extension_descriptor.number,
        key=extension_descriptor,
        new_default=lambda x: x.__class__(),
    )

    decoder(
        pos=0,
        end=len(payload),
        buffer=payload,
        message=Base(),
        field_dict={},
    )


def test_descriptor():
    assert Base.DESCRIPTOR.extension_ranges == [(100, 200)]
    assert Base.DESCRIPTOR.extensions_by_name == {}


def test_simple_extensions():
    message = Base()
    extension: FieldDescriptor = DESCRIPTOR.extensions_by_name["name"]

    assert message.HasExtension(extension) is False
    message.Extensions[extension] = "foo"
    assert message.HasExtension(extension)
    assert message.Extensions[extension] == "foo"
    assert list(message.Extensions) == [extension]
    payload = encode_extension(message, extension)
    assert len(payload) == 6

    assert encode_extensions(message) == {
        100: payload,
    }


def test_nested_extensions():
    message = Base()
    extension: FieldDescriptor = DESCRIPTOR.extensions_by_name["nested"]

    assert message.HasExtension(extension) is False
    message.Extensions[extension].MergeFrom(Nested(int_32_value=1))
    assert message.HasExtension(extension)
    assert message.Extensions[extension] == Nested(int_32_value=1)
    message.SerializeToString()

    assert list(message.Extensions) == [extension]
    payload = encode_extension(message, extension)
    assert len(payload) == 5

    payloads = encode_extensions(message)
    assert payloads == {103: payload}

    message_back = decode_extensions(Base(), payloads)
    assert message_back == message

    decode_extension(payload, extension)

    run_round_trip(message)


def test_nested_many():
    message = Base()

    message.Extensions[DESCRIPTOR.extensions_by_name["name"]] = "foo"
    message.Extensions[DESCRIPTOR.extensions_by_name["nested"]].MergeFrom(
        Nested(int_32_value=1)
    )
    message.Extensions[DESCRIPTOR.extensions_by_name["nested_repeated"]].append(
        Nested(int_32_value=1)
    )

    result = run_round_trip(message)
    assert result.Extensions[DESCRIPTOR.extensions_by_name["name"]] == "foo"
    assert result.Extensions[DESCRIPTOR.extensions_by_name["nested"]] == Nested(
        int_32_value=1
    )
    assert message.Extensions[DESCRIPTOR.extensions_by_name["nested_repeated"]] == [
        Nested(int_32_value=1)
    ]
