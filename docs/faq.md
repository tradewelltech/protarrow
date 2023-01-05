# FAQ

### Why convert from protobuf to arrow?

You need the right tool for the right job.
**Apache Arrow** is optimized for analytical tasks.
Whereas **protobuf** is optimized for transactional tasks.

**protarrow** allows you to convert from one format to the other seamlessly, deterministically and  without data loss.

Here are a few use cases:

1. Unified realtime and batch data processing

Transactional, real time services run using grpc or protobuf over kafka.
At the end of the day you want to run some analytical batch jobs using the same data.
Protarrow can convert protobuf data to arrow.
It can also help you convert parquet data generated from kafka connect back to protobuf.

2. Build realtime analytical and ML services using kafka and protobuf

You can use kafka to publish protobuf messages in real time. 
These messages can then be polled and processes in micro batches.
These batches can be converted to arrow tables seamlessly to run analytics or ML workload.
Later the data can be converted back to protobuf and published on kafka.

3. Unit Tests

For unit test relying on data sample, you can save your protobuf as json (or jsonl).
This data can then be parsed with high fidelity using the protobuf library, and converted to arrow Table.

4. Convert parquet data back to protobuf

If you use kafka-connect, your kafka topic data is archived as parquet file. 
To run test or replay your data you may want to convert this parquet data to protobuf.

### Why not use `pa.Table.from_list` and `google.protobuf.json_format.MessageToDict`

You could convert protobuf messages to arrow out of the box:
```python
import pyarrow as pa
import MyProto
from google.protobuf.json_format import MessageToDict

my_protos = [
    MyProto(name="foo", id=1, values=[1, 2, 4]),
    MyProto(name="bar", id=2, values=[3, 4, 5]),
]

jsons = [MessageToDict(message) for message in my_protos]

table = pa.Table.from_pylist(jsons)
```

This works, but it has a few drawbacks:

- It can't guess the types for missing values, empty list, empty map, empty input.
- Special types like date and timestamp are not supported.
- Integer and floats will be casted to there 64 bits representation, which is inefficient.
- When representing enum as string you'd want to use dictionary encoding to save memory.
