# Contributing

To get started developing `boostedblob`:
- Install [`poetry`](https://python-poetry.org/)
- Run `poetry install`, to create a virtualenv with all the dependencies you need
- Run `poetry shell` to get a shell with the virtualenv activated
- Run `./test.sh` to run tests

Note you'll need to have a functional Azure and Google Cloud situation for tests to pass. We use
[these locations](https://github.com/hauntsaninja/boostedblob/blob/af48ecc4250a0b7652f55a01c7aa7cfb35dc8694/tests/helpers/tmp_dir.py#L11)
for testing. Ask me for access / feel free to swap them out / make it configurable via env var.

To make a new release for `boostedblob`:
- Update the changelog
- Update the version in `pyproject.toml`
- Update the version in `boostedblob/__init__.py`
- Run `poetry publish --build`
