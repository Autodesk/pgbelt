# Pgbelt

<p align="center">
    <img src="https://github.com/Autodesk/pgbelt/blob/main/pgbelt.png?raw=true" width="400">
</p>

<p align="center">
    <a href="https://github.com/autodesk/pgbelt" target="_blank">
        <img src="https://img.shields.io/github/last-commit/autodesk/pgbelt" alt="Latest Commit">
    </a>
    <img src="https://github.com/Autodesk/pgbelt/actions/workflows/ci.yml/badge.svg">
    <a href="http://www.apache.org/licenses/LICENSE-2.0" target="_blank">
        <img src="https://img.shields.io/github/license/Autodesk/pgbelt">
    </a>
</p>

PgBelt is a CLI tool used to manage Postgres data migrations from beginning to end,
for a single database or a fleet, leveraging pglogical replication.

It was built to assist in migrating data between postgres databases with as
little application downtime as possible. It works in databases running different versions
of postgres and makes it easy to run many migrations in parallel during a single downtime.

| :exclamation: This is very important                                                                                                            |
| :---------------------------------------------------------------------------------------------------------------------------------------------- |
| As with all Data Migration tasks, **there is a risk of data loss**. Please ensure you have backed up your data before attempting any migrations |

## Installation

### Install From PyPi

It is recommended to install pgbelt inside a virtual environment:

- [pyenv](https://github.com/pyenv/pyenv)
- [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv)

You must also have:

- Postgres Client Tools (pg_dump, pg_restore). Mac: `brew install libpq`. Ubuntu: `sudo apt-get install postgresql-client`

Install pgbelt locally:

    pip3 install pgbelt

## Quickstart with Pgbelt

See [this doc](docs/quickstart.md)!

## Playbook

This playbook gets updated actively. If you have any issues, solutions could be found in [this playbook](docs/playbook.md).

## Contributing

We welcome contributions! See [this doc](CONTRIBUTING.md) on how to do so, including setting up your local development environment.
