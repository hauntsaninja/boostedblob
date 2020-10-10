set -ex

black --diff --check --quiet .
isort --diff --check --quiet .
flake8
POETRY_PYTHON=$(poetry run which python)
mypy boostedblob --python-executable $POETRY_PYTHON
mypy tests --disable-error-code var-annotated --python-executable $POETRY_PYTHON
pytest --cov=boostedblob
coverage html
