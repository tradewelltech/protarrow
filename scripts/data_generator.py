import json

from google.protobuf.json_format import MessageToDict

import tests.test_arrow_to_proto
from tests.random_generator import generate_messages


def main():
    for message_type in tests.test_arrow_to_proto.MESSAGES:
        messages = generate_messages(message_type, 20)
        file_name = message_type.DESCRIPTOR.name + ".jsonl"
        print(file_name)
        with open(file_name, "w") as fp:
            for message in messages:
                json.dump(MessageToDict(message, preserving_proto_field_name=True), fp)
                fp.write("\n")


if __name__ == "__main__":
    main()
