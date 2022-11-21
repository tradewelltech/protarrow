"""Example from the README.md"""
import pyarrow as pa
from example_pb2 import MyProto

import protarrow

my_protos = [
    MyProto(name="foo", values=[1, 2, 4]),
    MyProto(name="bar", values=[3, 4, 5]),
]

schema = protarrow.message_type_to_schema(MyProto)
table = protarrow.messages_to_table(my_protos, MyProto)
record_batch = protarrow.messages_to_record_batch(my_protos, MyProto)

# this requires pandas and tabulate installed
# print(table.to_pandas().to_markdown(index=False))


protos_from_record_batch = protarrow.record_batch_to_messages(record_batch, MyProto)
protos_from_table = protarrow.table_to_messages(table, MyProto)


config = protarrow.ProtarrowConfig(enum_type=pa.int32())

config = protarrow.ProtarrowConfig(
    timestamp_type=pa.timestamp("ms", "America/New_York")
)
