import pytest
from pytest_benchmark.fixture import BenchmarkFixture

from protarrow import messages_to_record_batch, record_batch_to_messages
from protarrow_protos.bench_pb2 import ExampleMessage
from tests.random_generator import generate_messages

SIZES = [10, 100, 1_000, 10_000]


@pytest.mark.parametrize("size", SIZES)
def test_messages_to_record_batch(benchmark: BenchmarkFixture, size):
    source_messages = generate_messages(ExampleMessage, size, 10)
    benchmark(messages_to_record_batch, source_messages, ExampleMessage)


@pytest.mark.parametrize("size", SIZES)
def test_record_batch_to_messages(benchmark: BenchmarkFixture, size):
    source_messages = generate_messages(ExampleMessage, size, 10)
    record_batch = messages_to_record_batch(source_messages, ExampleMessage)
    benchmark(record_batch_to_messages, record_batch, ExampleMessage)
