# `belt`

A tool to help manage postgres data migrations.

**Usage**:

```console
$ belt [OPTIONS] COMMAND [ARGS]...
```

**Options**:

- `--install-completion`: Install completion for the current shell.
- `--show-completion`: Show completion for the current shell, to copy it or customize the installation.
- `--help`: Show this message and exit.

**Commands**:

- `analyze`: Run ANALYZE in the destination database.
- `check-pkeys`: Print out lists of tables with and without...
- `dst-dsn`: Print a dsn to stdout that you can use to...
- `dump-schema`: Dumps and sanitizes the schema from the...
- `dump-tables`: Dump all tables without primary keys from the...
- `load-constraints`: Loads the NOT VALID constraints from the file...
- `load-schema`: Loads the sanitized schema from the file...
- `load-tables`: Load all locally saved table data files into...
- `precheck`: Report whether your source database meets the...
- `restore-logins`: Grant permission to log in for any user...
- `revoke-logins`: Discovers all users in the db who can log in,...
- `setup`: Configures pglogical to replicate all...
- `setup-back-replication`: Configures pglogical to replicate all...
- `src-dsn`: Print a dsn to stdout that you can use to...
- `status`: Print out a table of status information for...
- `sync`: Sync and validate all data that is not...
- `sync-sequences`: Retrieve the current value of all sequences...
- `sync-tables`: Dump and load all tables from the source...
- `teardown`: Removes all pglogical configuration from both...
- `teardown-back-replication`: Stops pglogical replication from the...
- `teardown-forward-replication`: Stops pglogical replication from the source...
- `validate-data`: Compares data in the source and target...

## `belt analyze`

Run ANALYZE in the destination database. This should be run after data is
completely replicated and before applications are allowed to use the new db.

Can be run with a null src in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt analyze [OPTIONS] DC [DB]
```

**Arguments**:

- `DC`: [required]
- `[DB]`

**Options**:

- `--help`: Show this message and exit.

## `belt check-pkeys`

Print out lists of tables with and without primary keys

**Usage**:

```console
$ belt check-pkeys [OPTIONS] DC DB
```

**Arguments**:

- `DC`: [required]
- `DB`: [required]

**Options**:

- `--help`: Show this message and exit.

## `belt dst-dsn`

Print a dsn to stdout that you can use to connect to the destination db:
psql "$(dbup dst-dsn scribble prod-use1-pg-1)"

Pass --owner to log in as the owner or --pglogical to log in as pglogical.

**Usage**:

```console
$ belt dst-dsn [OPTIONS] DC DB
```

**Arguments**:

- `DC`: [required]
- `DB`: [required]

**Options**:

- `--owner / --no-owner`: Use the owner credentials [default: False]
- `--pglogical / --no-pglogical`: Use the pglogical credentials. [default: False]
- `--help`: Show this message and exit.

## `belt dump-schema`

Dumps and sanitizes the schema from the source database, then saves it to
a file. Three files will be generated. One contains the entire sanitized
schema, one contains the schema with all NOT VALID constraints removed, and
another contains only the NOT VALID constraints that were removed. These
files will be saved in the schemas directory.

Requires both src and dst to be not null in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt dump-schema [OPTIONS] DC [DB]
```

**Arguments**:

- `DC`: [required]
- `[DB]`

**Options**:

- `--help`: Show this message and exit.

## `belt dump-tables`

Dump all tables without primary keys from the source database and save
them to files locally.

You may also provide a list of tables to dump with the
--tables option and only these tables will be dumped.

Can be run with a null dst in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt dump-tables [OPTIONS] DC [DB]
```

**Arguments**:

- `DC`: [required]
- `[DB]`

**Options**:

- `--tables TEXT`: Specific tables to dump [default: ]
- `--help`: Show this message and exit.

## `belt load-constraints`

Loads the NOT VALID constraints from the file schemas/dc/db/invalid_constraints.sql
into the destination as the owner user. This must only be done after all data is
synchronized from the source to the destination database.

Can be run with a null src in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt load-constraints [OPTIONS] DC [DB]
```

**Arguments**:

- `DC`: [required]
- `[DB]`

**Options**:

- `--help`: Show this message and exit.

## `belt load-schema`

Loads the sanitized schema from the file schemas/dc/db/no_invalid_constraints.sql
into the destination as the owner user.

Invalid constraints are omitted because the source database may contain data
that was created before the constraint was added. Loading the constraints into
the destination before the data will cause replication to fail.

Can be run with a null src in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt load-schema [OPTIONS] DC [DB]
```

**Arguments**:

- `DC`: [required]
- `[DB]`

**Options**:

- `--help`: Show this message and exit.

## `belt load-tables`

Load all locally saved table data files into the destination db. A table will
only be loaded into the destination if it currently contains no rows.

You may also provide a list of tables to load with the
--tables option and only these files will be loaded.

Can be run with a null src in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt load-tables [OPTIONS] DC [DB]
```

**Arguments**:

- `DC`: [required]
- `[DB]`

**Options**:

- `--tables TEXT`: Specific tables to load [default: ]
- `--help`: Show this message and exit.

## `belt precheck`

Report whether your source database meets the basic requirements for pgbelt.
Any red item in a row in the table indicates a requirement not satisfied by your db.
This command can not check network connectivity between your source and destination!

If a dbname is given this will also show whether the configuration of
the root and owner users seems ok and a summary of whether each
table and sequence in the database can be replicated.
If a row contains any red that sequence or table can not be replicated.

Can be run with a null dst in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt precheck [OPTIONS] DC [DB]
```

**Arguments**:

- `DC`: [required]
- `[DB]`

**Options**:

- `--help`: Show this message and exit.

## `belt restore-logins`

Grant permission to log in for any user present in the config file. The user
must already have a password. This will not generate or modify existing
passwords for users.

Intended to be used after revoke-logins in case a rollback is required.

Can be run with a null dst in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt restore-logins [OPTIONS] DC [DB]
```

**Arguments**:

- `DC`: [required]
- `[DB]`

**Options**:

- `--help`: Show this message and exit.

## `belt revoke-logins`

Discovers all users in the db who can log in, saves them in the config file,
then revokes their permission to log in. Use this command to ensure that all
writes to the source database have been stopped before syncing sequence values
and tables without primary keys.

Can be run with a null dst in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt revoke-logins [OPTIONS] DC [DB]
```

**Arguments**:

- `DC`: [required]
- `[DB]`

**Options**:

- `--help`: Show this message and exit.

## `belt setup`

Configures pglogical to replicate all compatible tables from the source
to the destination db. This includes copying the database schema from the
source into the destination.

If you want to set up the schema in the destination db manually you can use
the --no-schema option to stop this from happening.

Requires both src and dst to be not null in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt setup [OPTIONS] DC [DB]
```

**Arguments**:

- `DC`: [required]
- `[DB]`

**Options**:

- `--schema / --no-schema`: Copy the schema? [default: True]
- `--help`: Show this message and exit.

## `belt setup-back-replication`

Configures pglogical to replicate all compatible tables from the destination
to the source db. Can only complete successfully after the initial load phase
is completed for replication from the source to target.

Back replication ensures that dataloss does not occur if a rollback is required
after applications are allowed to begin writing data into the destination db.

Requires both src and dst to be not null in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt setup-back-replication [OPTIONS] DC [DB]
```

**Arguments**:

- `DC`: [required]
- `[DB]`

**Options**:

- `--help`: Show this message and exit.

## `belt src-dsn`

Print a dsn to stdout that you can use to connect to the source db:
psql "$(dbup src-dsn scribble prod-use1-pg-1)"

Pass --owner to log in as the owner or --pglogical to log in as pglogical.

**Usage**:

```console
$ belt src-dsn [OPTIONS] DC DB
```

**Arguments**:

- `DC`: [required]
- `DB`: [required]

**Options**:

- `--owner / --no-owner`: Use the owner credentials [default: False]
- `--pglogical / --no-pglogical`: Use the pglogical credentials. [default: False]
- `--help`: Show this message and exit.

## `belt status`

Print out a table of status information for one or all of the dbs in a datacenter.
Contains the pglogical replication status for both directions of replication and
replication lag data for forward replication. Possible replication statuses are as
follows:

unconfigured: No replication has been set up in this direction yet.

initializing: Pglogical is performing an initial data dump to bring the follower up to speed.
You can not begin replication in the opposite direction during this stage.

replicating: Pglogical is replicating only net new writes in this direction.

down: Pglogical has encountered an error and has stopped replicating entirely.
Check the postgres logs on both dbs to determine the cause.

Requires both src and dst to be not null in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt status [OPTIONS] DC [DB]
```

**Arguments**:

- `DC`: [required]
- `[DB]`

**Options**:

- `--help`: Show this message and exit.

## `belt sync`

Sync and validate all data that is not replicated with pglogical. This includes all
tables without primary keys and all sequences. Also loads any previously omitted
NOT VALID constraints into the destination db and runs ANALYZE in the destination.

This command is equivalent to running the following commands in order:
sync-sequences, sync-tables, validate-data, load-constraints, analyze.
Though here they may run concurrently when possible.

Requires both src and dst to be not null in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt sync [OPTIONS] DC [DB]
```

**Arguments**:

- `DC`: [required]
- `[DB]`

**Options**:

- `--help`: Show this message and exit.

## `belt sync-sequences`

Retrieve the current value of all sequences in the source database and update
the sequences in the target to match.

Requires both src and dst to be not null in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt sync-sequences [OPTIONS] DC [DB]
```

**Arguments**:

- `DC`: [required]
- `[DB]`

**Options**:

- `--help`: Show this message and exit.

## `belt sync-tables`

Dump and load all tables from the source database to the destination database.
Equivalent to running dump-tables followed by load-tables. Table data will be
saved locally in files.

You may also provide a list of tables to sync with the
--tables option and only these tables will be synced.

Requires both src and dst to be not null in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt sync-tables [OPTIONS] DC [DB]
```

**Arguments**:

- `DC`: [required]
- `[DB]`

**Options**:

- `--tables TEXT`: Specific tables to sync [default: ]
- `--help`: Show this message and exit.

## `belt teardown`

Removes all pglogical configuration from both databases. If any replication is
configured this will stop it.

If run with --full the pglogical users and extension will be dropped.

WARNING: running with --full may cause the database to lock up. You should be
prepared to reboot the database if you do this.

Requires both src and dst to be not null in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt teardown [OPTIONS] DC [DB]
```

**Arguments**:

- `DC`: [required]
- `[DB]`

**Options**:

- `--full / --no-full`: Remove pglogical user and extension [default: False]
- `--help`: Show this message and exit.

## `belt teardown-back-replication`

Stops pglogical replication from the destination database to the source.
You should only do this once you are certain a rollback will not be required.

Can be run with a null dst in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt teardown-back-replication [OPTIONS] DC [DB]
```

**Arguments**:

- `DC`: [required]
- `[DB]`

**Options**:

- `--help`: Show this message and exit.

## `belt teardown-forward-replication`

Stops pglogical replication from the source database to the destination.
This should be done during your migration downtime before writes are allowed
to the destination.

Can be run with a null src in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt teardown-forward-replication [OPTIONS] DC [DB]
```

**Arguments**:

- `DC`: [required]
- `[DB]`

**Options**:

- `--help`: Show this message and exit.

## `belt validate-data`

Compares data in the source and target databases. Both a random sample and a
sample of the latest rows will be compared for each table. Does not validate
the entire data set.

Requires both src and dst to be not null in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt validate-data [OPTIONS] DC [DB]
```

**Arguments**:

- `DC`: [required]
- `[DB]`

**Options**:

- `--help`: Show this message and exit.
