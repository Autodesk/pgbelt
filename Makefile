PYTHON_CODE_PATH="./pgbelt"

.DEFAULT_GOAL := help

# This help function will automatically generate help/usage text for any make target that is commented with "##".
# Targets with a singe "#" description do not show up in the help text
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-40s\033[0m %s\n", $$1, $$2}'

install: ## Install whatever you have locally
	pip3 install -e .

setup: ## Install development requirements. You should be in a virtualenv
	poetry install && pre-commit install

test: ## Run tests
	docker build . -t autodesk/pgbelt:latest && docker build tests/integration/files/postgres13-pglogical-docker/ -t autodesk/postgres-pglogical-docker:13 && docker-compose run tests

tests: test
