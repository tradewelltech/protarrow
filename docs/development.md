
# Development

## Set up

```shell
python3 -m venv --clear venv
source venv/bin/activate
poetry self add "poetry-dynamic-versioning[plugin]"
poetry install
python ./scripts/protoc.py
pre-commit install
```

## Testing

This library relies on property based testing. 
Tests convert randomly generated data from protobuf to arrow and back, making sure the end result is the same as the input.

```shell
coverage run --branch --include "*/protarrow/*" -m pytest tests
coverage report
```

## Resources

The repo set up is inspired by this [guide](https://mathspp.com/blog/how-to-create-a-python-package-in-2022)
