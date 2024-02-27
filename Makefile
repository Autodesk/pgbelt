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

local-dev: ## Sets up docker containers for Postgres DBs and gets you into a docker container with pgbelt installed. DC: testdc, DB: testdb
	docker build . -t autodesk/pgbelt:latest && docker build tests/integration/files/postgres13-pglogical-docker/ -t autodesk/postgres-pglogical-docker:13 && docker-compose run localtest

clean-docker: ## Stop and remove all docker containers and images made from local testing
	docker stop $$(docker ps -aq --filter name=^/pgbelt) && docker rm $$(docker ps -aq --filter name=^/pgbelt) && docker-compose down --rmi all

# Note: typer-cli has dependency conflict issues that don't affect it generating docs, see https://github.com/tiangolo/typer-cli/pull/120.
# We need to install the package with pip instead. Then, we run pre-commit to fix the formatting of the generated file.
# Finally, we OR true because the pre-commit errors when finding stuff to fix, but that's exactly what we want it to do.
generate-usage-docs: ## Generate usage docs
	pip3 install typer-cli && typer pgbelt/main.py utils docs --name belt > docs/usage.md && pre-commit run --files docs/usage.md || true
