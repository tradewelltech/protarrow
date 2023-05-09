import dataclasses
import pathlib

import inflection
from jinja2 import Environment, FileSystemLoader

DIR = pathlib.Path(__file__).parent


@dataclasses.dataclass(frozen=True)
class TypeTemplate:
    name: str
    protobuf_type: str

    @staticmethod
    def primitive(name: str) -> "TypeTemplate":
        return TypeTemplate(name, name)

    @staticmethod
    def logical(qualified_name: str) -> "TypeTemplate":
        return TypeTemplate(
            inflection.underscore(qualified_name.split(".")[-1]), qualified_name
        )

    @staticmethod
    def wrapped(name: str) -> "TypeTemplate":
        return TypeTemplate("wrapped_" + name.lower(), f"google.protobuf.{name}Value")


TYPES = [
    # Primitives
    TypeTemplate.primitive("double"),
    TypeTemplate.primitive("float"),
    TypeTemplate.primitive("int32"),
    TypeTemplate.primitive("int64"),
    TypeTemplate.primitive("uint32"),
    TypeTemplate.primitive("uint64"),
    TypeTemplate.primitive("sint32"),
    TypeTemplate.primitive("sint64"),
    TypeTemplate.primitive("fixed32"),
    TypeTemplate.primitive("fixed64"),
    TypeTemplate.primitive("sfixed32"),
    TypeTemplate.primitive("sfixed64"),
    TypeTemplate.primitive("bool"),
    TypeTemplate.primitive("string"),
    TypeTemplate.primitive("bytes"),
    # Wrapped:
    TypeTemplate.wrapped("Double"),
    TypeTemplate.wrapped("Float"),
    TypeTemplate.wrapped("Int32"),
    TypeTemplate.wrapped("Int64"),
    TypeTemplate.wrapped("UInt32"),
    TypeTemplate.wrapped("UInt64"),
    TypeTemplate.wrapped("Bool"),
    TypeTemplate.wrapped("String"),
    TypeTemplate.wrapped("Bytes"),
    # Others
    TypeTemplate.logical("ExampleEnum"),
    TypeTemplate.logical("google.protobuf.Timestamp"),
    TypeTemplate.logical("google.type.Date"),
    TypeTemplate.logical("google.type.TimeOfDay"),
    TypeTemplate.logical("google.protobuf.Empty"),
]

MAP_KEYS = ["int32", "string"]


def generate():
    env = Environment(loader=FileSystemLoader(DIR.as_posix()), autoescape=True)
    template = env.get_template("template.proto.in")
    generated = template.render(types=TYPES, map_keys=MAP_KEYS)
    with (DIR.parent / "protos" / "bench.proto").open("w") as fp:
        fp.write(generated)


if __name__ == "__main__":
    generate()
