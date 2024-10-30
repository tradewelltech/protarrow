from google.protobuf.empty_pb2 import Empty

from protarrow import ProtarrowConfig
from protarrow.proto_to_arrow import _messages_to_array


def test_messages_to_array():
    _messages_to_array([Empty(), Empty()], Empty.DESCRIPTOR, None, ProtarrowConfig())
