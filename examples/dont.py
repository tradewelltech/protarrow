"""Example from the documentation"""
import pyarrow as pa
from example_pb2 import MyProto
from google.protobuf.json_format import MessageToDict

my_protos = [
    MyProto(name="foo", id=1, values=[1, 2, 4]),
    MyProto(name="bar", id=2, values=[3, 4, 5]),
]

jsons = [MessageToDict(message) for message in my_protos]

table = pa.Table.from_pylist(jsons)

print(table.to_pandas().to_markdown(index=False))
