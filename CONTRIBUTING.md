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

Note you'll need to have a functional Azure and Google Cloud situation for tests to pass. We use
[these locations](https://github.com/hauntsaninja/boostedblob/blob/af48ecc4250a0b7652f55a01c7aa7cfb35dc8694/tests/helpers/tmp_dir.py#L11)
for testing. Ask me for access / feel free to swap them out.

## Debugging tricks

Set the environment variable `BBB_DEBUG=1` to get full tracebacks on error and debug logs.

## Releasing

To make a new release for `boostedblob`:
- Update the changelog
- Update the version in `pyproject.toml`
- Update the version in `boostedblob/__init__.py`
- Run `poetry publish --build`
- Tag the release on Github
