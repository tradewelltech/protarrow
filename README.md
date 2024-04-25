[![PyPI Version][pypi-image]][pypi-url]
[![Python Version][versions-image]][versions-url]
[![Github Stars][stars-image]][stars-url]
[![codecov][codecov-image]][codecov-url]
[![Build Status][build-image]][build-url]
[![Documentation][doc-image]][doc-url]
[![License][license-image]][license-url]
[![Downloads][downloads-image]][downloads-url]
[![Downloads][downloads-month-image]][downloads-month-url]
[![Code style: black][codestyle-image]][codestyle-url]
[![snyk][snyk-image]][snyk-url]


# Protarrow

**Protarrow** is a python library for converting from Protocol Buffers to Apache Arrow and back.

It is used at [Tradewell Technologies](https://www.tradewelltech.co/), 
to share data between transactional and analytical applications,
with little boilerplate code and zero data loss.

# Installation

```shell
pip install protarrow
```

# Usage

Taking a simple protobuf:

```protobuf
message MyProto {
  string name = 1;
  int32 id = 2;
  repeated int32 values = 3;
}
```

It can be converted to a `pyarrow.Table`:

```python
import protarrow

my_protos = [
    MyProto(name="foo", id=1, values=[1, 2, 4]),
    MyProto(name="bar", id=2, values=[3, 4, 5]),
]

table = protarrow.messages_to_table(my_protos, MyProto)
```


| name   |   id | values   |
|:-------|-----:|:---------|
| foo    |    1 | [1 2 4]  |
| bar    |    2 | [3 4 5]  |

And the table can be converted back to proto:

```python
protos_from_table = protarrow.table_to_messages(table, MyProto)
```

See the [documentation](https://protarrow.readthedocs.io/en/latest/)


<!-- Badges: -->

[pypi-image]: https://img.shields.io/pypi/v/protarrow
[pypi-url]: https://pypi.org/project/protarrow/
[build-image]: https://github.com/tradewelltech/protarrow/actions/workflows/ci.yaml/badge.svg
[build-url]: https://github.com/tradewelltech/protarrow/actions/workflows/ci.yaml
[stars-image]: https://img.shields.io/github/stars/tradewelltech/protarrow
[stars-url]: https://github.com/tradewelltech/protarrow
[versions-image]: https://img.shields.io/pypi/pyversions/protarrow
[versions-url]: https://pypi.org/project/protarrow/
[doc-image]: https://readthedocs.org/projects/protarrow/badge/?version=latest
[doc-url]: https://protarrow.readthedocs.io/en/latest/?badge=latest
[license-image]: http://img.shields.io/:license-Apache%202-blue.svg
[license-url]: https://github.com/tradewelltech/protarrow/blob/master/LICENSE
[codecov-image]: https://codecov.io/gh/tradewelltech/protarrow/branch/master/graph/badge.svg?token=XMFH27IL70
[codecov-url]: https://codecov.io/gh/tradewelltech/protarrow
[downloads-image]: https://pepy.tech/badge/protarrow
[downloads-url]: https://static.pepy.tech/badge/protarrow
[downloads-month-image]: https://pepy.tech/badge/protarrow/month
[downloads-month-url]: https://static.pepy.tech/badge/protarrow/month
[codestyle-image]: https://img.shields.io/badge/code%20style-black-000000.svg
[codestyle-url]: https://github.com/ambv/black
[snyk-image]: https://snyk.io/advisor/python/protarrow/badge.svg
[snyk-url]: https://snyk.io/advisor/python/protarrow
