# Contributing

## Setup

To get started developing `boostedblob`:
- Install [poetry](https://python-poetry.org/)
    - https://github.com/python-poetry/poetry#installation
    - Alternatively, `brew install poetry` or `pipx install poetry` work well
- Run `poetry install`, to create a virtualenv with all the dependencies you need
- Run `poetry shell` to get a shell with the virtualenv activated

## Running tests

Run `./test.sh` to run tests.

This will run tests with some hardcoded cloud locations, which you may not have access to. Instead,
use `BBB_TEST_LOCATIONS` to tell tests what locations to use. E.g.,
`BBB_TEST_LOCATIONS=az://some/location ./test.sh` will run Azure tests reading and writing to the
`az://some/location` and skip Google Cloud tests.

## Debugging tricks

Set the environment variable `BBB_DEBUG=1` to get full tracebacks on error and debug logs.

## Codebase tips

Here are some tips that should help navigating the codebase:
- [cli.py](https://github.com/hauntsaninja/boostedblob/blob/master/boostedblob/cli.py) is a good way
  to get to know how to use boostedblob as a library
- boostedblob leans quite heavily on the [single
  dispatch](https://docs.python.org/3/library/functools.html#functools.singledispatch) idiom. See
  pathdispatch in
  [path.py](https://github.com/hauntsaninja/boostedblob/blob/master/boostedblob/path.py) for details
- boost.py contains the relatively complex code that deals with regulating concurrency. Look at
  `read_stream` in
  [read.py](https://github.com/hauntsaninja/boostedblob/blob/master/boostedblob/read.py) as a simple
  example of how we use this abstraction

## Releasing

To make a new release for `boostedblob`:
- Update the changelog
- Update the version in `pyproject.toml`
- Update the version in `boostedblob/__init__.py`
- Run `poetry publish --build`
- Tag the release on Github
