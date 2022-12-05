import json
from pathlib import Path

from google.protobuf.json_format import MessageToDict

from protarrow_protos.bench_pb2 import ExampleMessage, NestedExampleMessage
from tests.random_generator import generate_messages

DIR = Path(__file__).parent


def main():
    for message_type in [ExampleMessage, NestedExampleMessage]:
        messages = generate_messages(message_type, 20)
        file_name = (
            DIR.parent / "tests" / "data" / (message_type.DESCRIPTOR.name + ".jsonl")
        ).as_posix()
        print(file_name)
        with open(file_name, "w") as fp:
            for message in messages:
                json.dump(MessageToDict(message, preserving_proto_field_name=True), fp)
                fp.write("\n")


if __name__ == "__main__":
    main()
