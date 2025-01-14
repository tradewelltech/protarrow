from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.internal.type_checkers import TYPE_TO_ENCODER

from protarrow_protos.extension_pb2 import DESCRIPTOR, Base


def test_extensions():
    foo = Base()
    print(dir(foo))
    print(foo.ListFields())

    name_extension: FieldDescriptor = DESCRIPTOR.extensions_by_name["name"]
    assert foo.HasExtension(name_extension) == False
    foo.Extensions[name_extension] = "foo"
    assert foo.Extensions[name_extension] == "foo"
    assert list(foo.Extensions) == [name_extension]
    for extension in foo.Extensions:
        TYPE_TO_ENCODER[extension.type](foo.Extensions[extension])

    assert foo.DESCRIPTOR.extension_ranges == [(100, 200)]
    assert foo.DESCRIPTOR.extensions_by_name == {}
