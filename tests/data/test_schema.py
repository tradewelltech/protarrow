import pyarrow as pa
import pytest

import protarrow
from protarrow_protos.bench_pb2 import ExampleMessage, NestedExampleMessage
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


def test_default_field_names_match_arrow():
    config = protarrow.ProtarrowConfig()
    assert pa.list_(pa.int32()).value_field.name == config.list_value_name
    assert pa.map_(pa.int32(), pa.string()).item_field.name == config.map_value_name


def test_nullability():
    schema = protarrow.message_type_to_schema(ExampleMessage)
    assert not schema.field("double_value").nullable
    assert not schema.field("double_values").nullable
    assert schema.field("wrapped_double_value").nullable

    nested_schema = pa.schema(
        list(
            protarrow.message_type_to_schema(NestedExampleMessage)
            .field("example_message")
            .type
        )
    )
    assert not nested_schema.field("double_value").nullable
    assert not nested_schema.field("double_values").nullable
    assert nested_schema.field("wrapped_double_value").nullable

    assert schema == nested_schema, (
        "The schema of a nested message is the same as if the message wasn't nested"
    )


@pytest.mark.parametrize("list_nullable", [True, False])
def test_list_nullable_config(list_nullable: bool):
    schema = protarrow.message_type_to_schema(
        ExampleMessage, protarrow.ProtarrowConfig(list_nullable=list_nullable)
    )
    assert schema.field("double_values").nullable == list_nullable


@pytest.mark.parametrize("map_nullable", [True, False])
def test_map_nullable_config(map_nullable: bool):
    schema = protarrow.message_type_to_schema(
        ExampleMessage, protarrow.ProtarrowConfig(map_nullable=map_nullable)
    )
    assert schema.field("double_string_map").nullable == map_nullable


def test_map_nullability():
    map_type: pa.MapType = pa.map_(pa.string(), pa.int32())
    assert map_type.key_field.nullable is False
    assert map_type.item_field.nullable is True


@pytest.mark.parametrize("map_value_nullable", [True, False])
def test_map_value_nullable_config(map_value_nullable: bool):
    schema = protarrow.message_type_to_schema(
        ExampleMessage, protarrow.ProtarrowConfig(map_value_nullable=map_value_nullable)
    )
    assert (
        schema.field("double_string_map").type.item_field.nullable == map_value_nullable
    )


@pytest.mark.parametrize("list_value_nullable", [True, False])
def test_list_value_nullable_config(list_value_nullable: bool):
    schema = protarrow.message_type_to_schema(
        ExampleMessage,
        protarrow.ProtarrowConfig(list_value_nullable=list_value_nullable),
    )
    assert (
        schema.field("double_values").type.value_field.nullable == list_value_nullable
    )


@pytest.mark.parametrize("list_value_name", ["foo", "bar"])
def test_list_value_name_config(list_value_name: str):
    schema = protarrow.message_type_to_schema(
        ExampleMessage, protarrow.ProtarrowConfig(list_value_name=list_value_name)
    )
    assert schema.field("double_values").type.value_field.name == list_value_name


@pytest.mark.parametrize("map_value_name", ["foo", "bar"])
def test_map_value_name_config(map_value_name: str):
    schema = protarrow.message_type_to_schema(
        ExampleMessage, protarrow.ProtarrowConfig(map_value_name=map_value_name)
    )
    assert schema.field("double_string_map").type.item_field.name == map_value_name
