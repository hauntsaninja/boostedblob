set -ex

black --diff --check --quiet .
isort --diff --check --quiet .
flake8
mypy boostedblob
mypy tests --disable-error-code var-annotated
pytest --cov=boostedblob
coverage html
