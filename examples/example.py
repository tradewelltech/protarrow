"""Example from the documentation"""
import pyarrow as pa
from example_pb2 import MyProto

import protarrow

my_protos = [
    MyProto(name="foo", id=1, values=[1, 2, 4]),
    MyProto(name="bar", id=2, values=[3, 4, 5]),
]

schema = protarrow.message_type_to_schema(MyProto)
table = protarrow.messages_to_table(my_protos, MyProto)
record_batch = protarrow.messages_to_record_batch(my_protos, MyProto)

# this requires pandas and tabulate installed
# print(table.to_pandas().to_markdown(index=False))


protos_from_record_batch = protarrow.record_batch_to_messages(record_batch, MyProto)
protos_from_table = protarrow.table_to_messages(table, MyProto)


config = protarrow.ProtarrowConfig(
    enum_type=pa.int32(),
    timestamp_type=pa.timestamp("ms", "America/New_York"),
    time_of_day_type=pa.time32("ms"),
)
record_batch = protarrow.messages_to_record_batch(my_protos, MyProto, config)


source_table = pa.table({"name": ["hello"]})
casted_table = protarrow.cast_table(source_table, MyProto, config)

# print(casted_table.to_pandas().to_markdown(index=False))
