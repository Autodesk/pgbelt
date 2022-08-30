# Contributing to pgbelt

Thank you for your interest in improving this project. This project is open-source under the `MIT license` and welcomes contributions in the form of bug reports, feature requests, and pull requests. We are excited and eager to accept external contributions!

## Filing Bugs

Report bugs on the `Issue Tracker`.

When filing an issue, make sure to answer these questions:

- Which operating system and Python version are you using?
- Which version of this project are you using?
- What did you do?
- What did you expect to see?
- What did you see instead?

The best way to get your bug fixed is to provide a test case, and/or steps to reproduce the issue. There is an issue template made for this repository so you can provide the needed information.

## Feature Requests

We accept feature requests! Please file requests in the `Issue Tracker`.

## Code Contributions

We accept external code contributions!

### Contributor License Agreement

Before contributing any code to this project, we kindly ask you to sign a Contributor License Agreement (CLA). We can not accept any pull request if a CLA has not been signed.

- If you are contributing on behalf of yourself, the CLA signature is included as a part of the pull request process.

- If you are contributing on behalf of your employer, please sign our [Corporate Contributor License](https://github.com/Autodesk/autodesk.github.io/releases/download/1.0/ADSK.Form.Corp.Contrib.Agmt.for.Open.Source.docx) Agreement. The document includes instructions on where to send the completed forms to. Once a signed form has been received, we can happily review and accept your pull requests.

### How to set up your development environment

It is recommended to install pgbelt inside a virtual environment if installing by clone:

- [pyenv](https://github.com/pyenv/pyenv)
- [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv)

You must also have:

- [poetry](https://github.com/python-poetry/poetry)
- Postgres Client Tools (pg_dump, pg_restore). Mac: `brew install libpq`. Ubuntu: `sudo apt-get install postgresql-client`
- [docker](https://www.docker.com/)

Install the egg locally:

    # create a python virtualenv with python 3.9.11 and activate it (any 3.9.x is ok)
    pyenv install 3.9.11
    pyenv virtualenv 3.9.11 pgbelt
    pyenv activate pgbelt

    # Install poetry inside your virtualenv
    pip3 install poetry

    # clone the repo
    git clone git@github.com:Autodesk/pgbelt.git
    cd pgbelt

    # install pgbelt and dev tools with make **setup**
    make setup

### How to test the project

You will want to run the full test suite (including integration tests) to ensure your contribution causes no issues.

To do this, this repository uses `docker` and `docker-compose` to run tests and set up a local migration scenario with multiple databases, to do a full migration run-through.

Simply run:

    make test

Tests are made with `pytest` and are in the `tests/` folder. The integration test is found in `tests/integration/`, along with accompanying files, such as the Dockerfile for Postgres with pglogical configured.

### How to submit changes

Open a `pull request` to submit changes to this project.

Your pull request needs to meet the following guidelines for acceptance:

- The Github Actions CI job must pass without errors and warnings.
- If your changes add functionality, update the documentation accordingly.

It is recommended to open an issue before starting work on anything. This will allow a chance to talk it over with the owners and validate your approach.
