# `belt`

A tool to help manage postgres data migrations.

**Usage**:

```console
$ belt [OPTIONS] COMMAND [ARGS]...
```

**Options**:

* `--install-completion`: Install completion for the current shell.
* `--show-completion`: Show completion for the current shell, to copy it or customize the installation.
* `--help`: Show this message and exit.

**Commands**:

* `connections`: Print out a table showing active database...
* `src-dsn`: Print a dsn to stdout that you can use to...
* `dst-dsn`: Print a dsn to stdout that you can use to...
* `check-pkeys`: Print out lists of tables with and without...
* `check-connectivity`: Returns exit code 0 if pgbelt can connect...
* `revoke-logins`: Discovers all users in the db who can log...
* `restore-logins`: Grant permission to log in for any user...
* `precheck`: Report whether your source database meets...
* `reset`: Reset an in-progress migration before...
* `dump-schema`: Dumps and sanitizes the schema from the...
* `load-schema`: Loads the sanitized schema from the file...
* `load-constraints`: Loads the NOT VALID constraints from the...
* `remove-constraints`: Removes NOT VALID constraints from the...
* `remove-indexes`: Removes indexes from the target database.
* `create-indexes`: Creates indexes from the file...
* `diff-schemas`: Compare source and destination schemas...
* `setup`: Configures pglogical to replicate all...
* `setup-back-replication`: Configures pglogical to replicate all...
* `status`: Print out a table of status information...
* `sync-sequences`: Sync all sequences to the destination...
* `sync-tables`: Dump tables without primary keys from the...
* `analyze`: Run ANALYZE in the destination database.
* `validate-data`: Compares data in the source and target...
* `sync`: Sync and validate all data that is not...
* `teardown-back-replication`: Stops pglogical replication from the...
* `teardown-forward-replication`: Stops pglogical replication from the...
* `teardown`: Removes all pglogical configuration from...

## `belt connections`

Print out a table showing active database connections for each database pair.
Displays the connection count and list of connected usernames for both source
and destination databases.

Always excludes &#x27;rdsadmin&#x27; and &#x27;postgres&#x27; users from the count.
Use --exclude-user to exclude additional specific usernames.
Use --exclude-pattern to exclude usernames matching LIKE patterns (e.g. &#x27;%%repuser%%&#x27;).

Example:
    belt connections testdc --exclude-user datadog --exclude-pattern &#x27;%%repuser%%&#x27;


Requires both src and dst to be not null in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt connections [OPTIONS] DC [DB]
```

**Arguments**:

* `DC`: [required]
* `[DB]`

**Options**:

* `-e, --exclude-user TEXT`: Additional usernames to exclude (can be repeated). Always excludes rdsadmin and postgres.
* `-p, --exclude-pattern TEXT`: LIKE patterns to exclude usernames (e.g. &#x27;%%repuser%%&#x27;). Can be repeated.
* `--help`: Show this message and exit.

## `belt src-dsn`

Print a dsn to stdout that you can use to connect to the source db:
psql &quot;$(dbup src-dsn scribble prod-use1-pg-1)&quot;

Pass --owner to log in as the owner or --pglogical to log in as pglogical.

**Usage**:

```console
$ belt src-dsn [OPTIONS] DC DB
```

**Arguments**:

* `DC`: [required]
* `DB`: [required]

**Options**:

* `--owner / --no-owner`: Use the owner credentials  [default: no-owner]
* `--pglogical / --no-pglogical`: Use the pglogical credentials.  [default: no-pglogical]
* `--help`: Show this message and exit.

## `belt dst-dsn`

Print a dsn to stdout that you can use to connect to the destination db:
psql &quot;$(dbup dst-dsn scribble prod-use1-pg-1)&quot;

Pass --owner to log in as the owner or --pglogical to log in as pglogical.

**Usage**:

```console
$ belt dst-dsn [OPTIONS] DC DB
```

**Arguments**:

* `DC`: [required]
* `DB`: [required]

**Options**:

* `--owner / --no-owner`: Use the owner credentials  [default: no-owner]
* `--pglogical / --no-pglogical`: Use the pglogical credentials.  [default: no-pglogical]
* `--help`: Show this message and exit.

## `belt check-pkeys`

Print out lists of tables with and without primary keys

**Usage**:

```console
$ belt check-pkeys [OPTIONS] DC DB
```

**Arguments**:

* `DC`: [required]
* `DB`: [required]

**Options**:

* `--help`: Show this message and exit.

## `belt check-connectivity`

Returns exit code 0 if pgbelt can connect to all databases in a datacenter
(if db is not specified), or to both src and dst of a database.

This is done by checking network access to the database ports ONLY.

If any connection times out, the command will exit 1. It will test ALL connections
before returning exit code 1 or 0, and output which connections passed/failed.


Requires both src and dst to be not null in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt check-connectivity [OPTIONS] DC [DB]
```

**Arguments**:

* `DC`: [required]
* `[DB]`

**Options**:

* `--help`: Show this message and exit.

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

* `DC`: [required]
* `[DB]`

**Options**:

* `--help`: Show this message and exit.

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

* `DC`: [required]
* `[DB]`

**Options**:

* `--help`: Show this message and exit.

## `belt precheck`

Report whether your source database meets the basic requirements for pgbelt.
Any red item in a row in the table indicates a requirement not satisfied by your db.
This command can not check network connectivity between your source and destination!

If a dbname is given this will also show whether the configuration of
the root and owner users seems ok and a summary of whether each
table and sequence in the database can be replicated.
If a row contains any red that sequence or table can not be replicated.


Requires both src and dst to be not null in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt precheck [OPTIONS] DC [DB]
```

**Arguments**:

* `DC`: [required]
* `[DB]`

**Options**:

* `--help`: Show this message and exit.

## `belt reset`

Reset an in-progress migration before cutover so replication can be started
again from the beginning.

This command:
1) Stops forward replication
2) Ensures reverse replication is stopped
3) Truncates destination tables (all tables in schema, or only config.tables)
4) Removes indexes from the destination
5) Removes NOT VALID constraints from the destination

Note: sequence values are intentionally left unchanged. They only need to be
synchronized after cutover by running sync-sequences.


Requires both src and dst to be not null in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt reset [OPTIONS] DC [DB]
```

**Arguments**:

* `DC`: [required]
* `[DB]`

**Options**:

* `--help`: Show this message and exit.

## `belt dump-schema`

Dumps and sanitizes the schema from the source database, then saves it to
a file. Four files will be generated:
1. The entire sanitized schema
2. The schema with all NOT VALID constraints and CREATE INDEX statements removed,
3. A file that contains only the CREATE INDEX statements
4. A file that contains only the NOT VALID constraints
These files will be saved in the schemas directory.


Requires both src and dst to be not null in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt dump-schema [OPTIONS] DC [DB]
```

**Arguments**:

* `DC`: [required]
* `[DB]`

**Options**:

* `--help`: Show this message and exit.

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

* `DC`: [required]
* `[DB]`

**Options**:

* `--help`: Show this message and exit.

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

* `DC`: [required]
* `[DB]`

**Options**:

* `--help`: Show this message and exit.

## `belt remove-constraints`

Removes NOT VALID constraints from the target database. This must be done
before setting up replication, and should only be used if the schema in the
target database was loaded outside of pgbelt.


Can be run with a null src in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt remove-constraints [OPTIONS] DC [DB]
```

**Arguments**:

* `DC`: [required]
* `[DB]`

**Options**:

* `--help`: Show this message and exit.

## `belt remove-indexes`

Removes indexes from the target database. This must be done
before setting up replication, and should only be used if the schema in the
target database was loaded outside of pgbelt.


Can be run with a null src in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt remove-indexes [OPTIONS] DC [DB]
```

**Arguments**:

* `DC`: [required]
* `[DB]`

**Options**:

* `--help`: Show this message and exit.

## `belt create-indexes`

Creates indexes from the file schemas/dc/db/indexes.sql into the destination
as the owner user. This must only be done after most data is synchronized
(at minimum after the initializing phase) from the source to the destination
database.

After creating indexes, the destination database should be analyzed to ensure
the query planner has the most up-to-date statistics for the indexes.


Can be run with a null src in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt create-indexes [OPTIONS] DC [DB]
```

**Arguments**:

* `DC`: [required]
* `[DB]`

**Options**:

* `--help`: Show this message and exit.

## `belt diff-schemas`

Compare source and destination schemas using pg_dump, filtered through
shell grep pipelines independent of pgbelt&#x27;s internal schema parser.

By default, NOT VALID constraints and CREATE INDEX statements are excluded
from the comparison since pgbelt loads those in separate steps. Use --full
to include them.

DBs with a table list configured are skipped since they represent subset
migrations where schemas will naturally differ.


Requires both src and dst to be not null in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt diff-schemas [OPTIONS] DC [DB]
```

**Arguments**:

* `DC`: [required]
* `[DB]`

**Options**:

* `--full`: Include NOT VALID constraints and CREATE INDEX statements in the diff. Without this flag, those are excluded since they are loaded in separate steps.
* `--help`: Show this message and exit.

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

* `DC`: [required]
* `[DB]`

**Options**:

* `--schema / --no-schema`: Copy the schema?  [default: schema]
* `--help`: Show this message and exit.

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

* `DC`: [required]
* `[DB]`

**Options**:

* `--help`: Show this message and exit.

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

* `DC`: [required]
* `[DB]`

**Options**:

* `--help`: Show this message and exit.

## `belt sync-sequences`

Sync all sequences to the destination database.

For sequences that back primary key columns, the value is set from
max(pk_column) on the destination table — this is always the safest baseline.

For all other sequences, the current value is read from the source and applied
to the destination, but only if the source value is &gt;= the current destination
value. This prevents regressing sequences if run after cutover.


Requires both src and dst to be not null in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt sync-sequences [OPTIONS] DC [DB]
```

**Arguments**:

* `DC`: [required]
* `[DB]`

**Options**:

* `--stride INTEGER`: Pad non-PK sequences by this amount when syncing: loads source_value + stride. Recommended default: --stride 1000.
* `--help`: Show this message and exit.

## `belt sync-tables`

Dump tables without primary keys from the source and pipe them directly
into the destination database. No intermediate files are used -- data is
streamed via pg_dump | psql.

A table will only be loaded into the destination if it currently contains
no rows.

You may also provide specific PK-less tables to sync with the --table option.
Need to run like --table table1 --table table2 ...


Requires both src and dst to be not null in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt sync-tables [OPTIONS] DC [DB]
```

**Arguments**:

* `DC`: [required]
* `[DB]`

**Options**:

* `--table TEXT`: Specific tables to sync
* `--help`: Show this message and exit.

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

* `DC`: [required]
* `[DB]`

**Options**:

* `--help`: Show this message and exit.

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

* `DC`: [required]
* `[DB]`

**Options**:

* `--help`: Show this message and exit.

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

* `DC`: [required]
* `[DB]`

**Options**:

* `--no-schema / --no-no-schema`: [default: no-no-schema]
* `--help`: Show this message and exit.

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

* `DC`: [required]
* `[DB]`

**Options**:

* `--help`: Show this message and exit.

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

* `DC`: [required]
* `[DB]`

**Options**:

* `--help`: Show this message and exit.

## `belt teardown`

Removes all pglogical configuration from both databases. If any replication is
configured this will stop it. It will also drop the pglogical user.

If run with --full the pglogical extension will be dropped.

WARNING: running with --full may cause the database to lock up. You should be
prepared to reboot the database if you do this.


Requires both src and dst to be not null in the config file.

If the db name is not given run on all dbs in the dc.

**Usage**:

```console
$ belt teardown [OPTIONS] DC [DB]
```

**Arguments**:

* `DC`: [required]
* `[DB]`

**Options**:

* `--full / --no-full`: Remove pglogical extension  [default: no-full]
* `--help`: Show this message and exit.

