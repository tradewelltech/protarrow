# Contributing

Welcome! We're happy to have you here. Thank you in advance for your contribution to protarrow.

The repo ships a [`justfile`](https://github.com/casey/just) with the common
development commands. Run `just --list` to see them all.

## Development environment set up

```shell
just setup
```

This runs `uv sync --all-groups`, regenerates the proto-derived python files,
and installs the git pre-commit hooks (via [`prek`](https://github.com/j178/prek)).

If you only need to regenerate the proto-derived python files, use:

```shell
just proto
```

## Testing

This library relies on property based testing.
Tests convert randomly generated data from protobuf to arrow and back, making sure the end result is the same as the input.

The tests take a long time to run, so `just test` runs them in parallel:

```shell
just test
```

Extra pytest arguments are forwarded:

```shell
just test -k test_my_thing
```

To get coverage:

```shell
just coverage
```

## Linting

```shell
just lint
```

## New Release

Create new releases in github and autogenerate the change log there.

## Testing the documentation

```shell
just docs-serve
```

To produce a static build:

```shell
just docs-build
```

## Updating dependencies

```shell
just update
```

This upgrades the `uv.lock`, recompiles `docs/requirements.txt`, and runs
`prek autoupdate`.

## Resources

The repo set up is inspired by this [guide](https://mathspp.com/blog/how-to-create-a-python-package-in-2022)
