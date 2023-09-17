"""
Tests the behavior of pyarrow
"""
import datetime

import pyarrow as pa
import pytest


def test_pyarrow_list_array_slices():
    values = [[1], [1, 2], [1, 2, 3]]
    array = pa.array(values)
    assert array.to_pylist() == values
    assert array.values.to_pylist() == [1, 1, 2, 1, 2, 3]
    assert array.offsets.to_pylist() == [0, 1, 3, 6]

    slice = array[1:]
    assert slice.to_pylist() == [[1, 2], [1, 2, 3]]
    assert slice.values == array.values  # Does not respect offset
    assert slice.value_parent_indices().to_pylist() == [0, 0, 1, 1, 1]
    assert slice.offsets.to_pylist() == [1, 3, 6]


def test_pyarrow_struct_array_slices():
    """Correct behaviour"""
    values = [
        {"int": 1, "str": "ABC"},
        {"int": 2, "str": "DEF"},
        {"int": 3, "str": "GHI"},
    ]
    array = pa.array(values)
    assert array.to_pylist() == values
    assert array.field(1).to_pylist() == ["ABC", "DEF", "GHI"]

    slice = array[1:]
    assert slice.to_pylist() == values[1:]
    assert slice.field(1).offset == 1


def test_nested_repeated():
    struct = pa.struct([pa.field("ints", pa.list_(pa.int32()))])
    array = pa.array(
        [
            {"ints": [1]},
            {"ints": [1, 2]},
            {"ints": [1, 2, 3]},
        ],
        type=struct,
    )
    slice = array[1:]
    assert slice.field(0).values.to_pylist() == [1, 1, 2, 1, 2, 3]
    assert slice.to_pylist() == [{"ints": [1, 2]}, {"ints": [1, 2, 3]}]
    assert slice.offset == 1
    assert slice.field(0).offset == 1
    assert slice.field(0).values.offset == 0


def test_pyarrow_gh_36809():
    """https://github.com/apache/arrow/issues/36809"""
    assert pa.scalar(
        [("foo", "bar")],
        pa.map_(
            pa.string(),
            pa.field("value", pa.string()),
        ),
    ).as_py() == [("foo", "bar")]

    with pytest.raises(KeyError, match=r"value"):
        pa.scalar(
            [("foo", "bar")],
            pa.map_(
                pa.string(),
                pa.field("map_value", pa.string()),
            ),
        ).as_py()


def test_empty_struct_now_possible():
    """See https://github.com/apache/arrow/issues/15109"""
    array = pa.StructArray.from_arrays(arrays=[], names=[], mask=pa.array([True, True]))
    assert array.type == pa.struct([])
    assert len(array) == 2


def test_empty_struct_workaround():
    array = pa.StructArray.from_arrays(
        arrays=[pa.nulls(2, pa.null())], names=["DELETE"], mask=pa.array([True, True])
    )
    assert len(array) == 2
    empty_array = array.cast(pa.struct([]))
    assert len(empty_array) == 2
    assert empty_array.type == pa.struct([])


def test_arrow_bug_18264():
    """https://issues.apache.org/jira/browse/ARROW-18264"""
    time_ns = pa.array([1], pa.time64("ns"))
    scalar = time_ns[0]
    assert scalar.as_py() == datetime.time(0, 0)
    assert scalar.value == 1


def test_arrow_bug_18257():
    """https://issues.apache.org/jira/browse/ARROW-18257"""
    dtype = pa.time64("ns")
    time_array = pa.array([1, 2, 3], dtype)
    assert pa.types.is_time64(time_array.type) is True
    assert isinstance(dtype, pa.Time64Type) is True
    assert isinstance(time_array.type, pa.Time64Type)
    assert dtype == time_array.type
    assert dtype.unit == "ns"
    assert time_array.type.unit == "ns"


def test_map_array_members():
    map_array = pa.array(
        [{"hello": 1, "foo": 2}, {"other": 3, "none": None}],
        pa.map_(pa.string(), pa.int32()),
    )
    assert isinstance(map_array, pa.MapArray)
    assert map_array.keys == pa.array(["hello", "foo", "other", "none"])
    assert map_array.items == pa.array([1, 2, 3, None], pa.int32())
