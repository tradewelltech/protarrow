"""Example from the README.md"""

from protarrow import messages_to_table, table_to_messages
from protarrow_protos.example_pb2 import MyProto

my_protos = [
    MyProto(name="foo", values=[1, 2, 4]),
    MyProto(name="bar", values=[3, 4, 5]),
]

table = messages_to_table(my_protos, MyProto)

# TODO: this requires pandas and tabulate installed
# print(table.to_pandas().to_markdown())


protos_from_table = table_to_messages(table, MyProto)
