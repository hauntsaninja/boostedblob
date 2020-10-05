set -ex

black --diff --check --quiet .
isort --diff --check --quiet .
flake8
# hack for prerelease mypy
mypy --version | grep 0.790 || pip install git+https://github.com/python/mypy.git
POETRY_PYTHON=$(poetry run which python)
mypy boostedblob --python-executable $POETRY_PYTHON
mypy tests --disable-error-code var-annotated --python-executable $POETRY_PYTHON
pytest --cov=boostedblob
coverage html
