set -ex

# TODO: maybe get rid of this script in favour of tox

black --diff --check --quiet .
isort --diff --check --quiet .
ruff check .
mypy boostedblob
mypy tests --disable-error-code var-annotated

if [[ -z ${BBB_TEST_LOCATIONS+is_unset} ]]; then
    export BBB_TEST_LOCATIONS="az://shantanutest/container"
fi

pytest --cov=boostedblob
coverage html
