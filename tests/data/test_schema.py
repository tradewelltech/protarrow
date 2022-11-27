import pyarrow as pa

import protarrow
from protarrow_protos.example_pb2 import NullableExample
from protarrow_protos.simple_pb2 import NestedTestMessage, TestMessage


def test_nullable():
    assert protarrow.message_type_to_schema(NullableExample) == pa.schema(
        [
            pa.field("int_value", pa.int32(), nullable=False),
            pa.field(
                "int_repeated",
                pa.list_(pa.field("item", pa.int32(), nullable=False)),
                nullable=False,
            ),
            pa.field(
                "int_map",
                pa.map_(pa.string(), pa.field("item", pa.int32(), nullable=False)),
                nullable=False,
            ),
            pa.field(
                "message_value",
                pa.struct(
                    [
                        pa.field(
                            "nested_int",
                            pa.int32(),
                            nullable=False,
                        )
                    ]
                ),
                nullable=True,
            ),
            pa.field(
                "message_repeated",
                pa.list_(
                    pa.field(
                        "item",
                        pa.struct(
                            [
                                pa.field(
                                    "nested_int",
                                    pa.int32(),
                                    nullable=False,
                                )
                            ]
                        ),
                        nullable=False,
                    )
                ),
                nullable=False,
            ),
            pa.field(
                "message_map",
                pa.map_(
                    pa.string(),
                    pa.field(
                        "item",
                        pa.struct(
                            [
                                pa.field(
                                    "nested_int",
                                    pa.int32(),
                                    nullable=False,
                                )
                            ]
                        ),
                        nullable=False,
                    ),
                ),
                nullable=False,
            ),
        ]
    )


def test_field_names():
    assert pa.list_(pa.int32()).value_field.name == "item"
    assert pa.map_(pa.int32(), pa.string()).key_field.name == "key"
    assert pa.map_(pa.int32(), pa.string()).item_field.name == "value"


def test_nullability():
    schema = protarrow.message_type_to_schema(TestMessage)
    assert not schema.field("double_value").nullable
    assert not schema.field("double_values").nullable
    assert schema.field("wrapped_double").nullable

    nested_schema = pa.schema(
        list(
            protarrow.message_type_to_schema(NestedTestMessage)
            .field("test_message")
            .type
        )
    )
    assert not nested_schema.field("double_value").nullable
    assert not nested_schema.field("double_values").nullable
    assert nested_schema.field("wrapped_double").nullable

    assert (
        schema == nested_schema
    ), "The schema of a nested message is the same as if the message wasn't nested"
