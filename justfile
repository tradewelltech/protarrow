# Set up the development environment
setup:
    uv sync --all-groups
    uv run python ./scripts/generate_proto.py
    uv run python ./scripts/protoc.py
    uvx prek install

# Regenerate proto-derived python files
proto:
    uv run python ./scripts/generate_proto.py
    uv run python ./scripts/protoc.py

# Run tests in parallel (extra args forwarded to pytest)
test *args:
    uv run pytest --numprocesses=auto -p no:benchmark ./tests {{ args }}

# Run tests with coverage and report
coverage:
    uv run coverage run --branch --source=protarrow -m pytest tests
    uv run coverage report --show-missing

# Run linting (prek on all files)
lint:
    uvx prek run --all-files

# Build the documentation
docs-build:
    uv run mkdocs build

# Serve the documentation with live reload
docs-serve:
    uv run mkdocs serve --livereload --watch=./


# Update all dependencies
update:
    uv lock --upgrade
    uv pip compile docs/requirements.in -o docs/requirements.txt
    uvx prek autoupdate
