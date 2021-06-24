set -ex

black --diff --check --quiet .
isort --diff --check --quiet .
flake8
mypy boostedblob
mypy tests --disable-error-code var-annotated

if [[ -z ${BBB_TEST_LOCATIONS+is_unset} ]]; then
    export BBB_TEST_LOCATIONS="az://shantanutest/container"
fi

pytest --cov=boostedblob
coverage html
