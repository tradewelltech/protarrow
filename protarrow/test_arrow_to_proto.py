from typing import cast

import pyarrow as pa

from protarrow.arrow_to_proto import ListValuesIterator, OffsetsIterator


def test_offsets_iterator():
    array = cast(pa.array([[1], [], None, [1, 2], [1, 2, 3]]), pa.ListArray)

    assert list(OffsetsIterator(array.offsets)) == [
        (0, 1),
        (1, 1),
        (1, 1),
        (1, 3),
        (3, 6),
    ]

    assert list(OffsetsIterator(array[1:].offsets)) == [
        (1, 1),
        (1, 1),
        (1, 3),
        (3, 6),
    ]

    assert list(OffsetsIterator(array[:-2].offsets)) == [
        (0, 1),
        (1, 1),
        (1, 1),
    ]


def test_list_value_iterator():
    array = pa.array([[1], [], None, [None], [1, 2], [1, 2, 3]])
    assert isinstance(array, pa.ListArray)
    assert list(ListValuesIterator(array)) == [
        pa.scalar(i, pa.int64()) for i in [1, None, 1, 2, 1, 2, 3]
    ]
