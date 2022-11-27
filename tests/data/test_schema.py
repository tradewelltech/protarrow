import pyarrow as pa

import protarrow
from protarrow_protos.example_pb2 import NullableExample


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
