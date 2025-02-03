# Playbook

## I see an incorrect credential error with the `pglogical` user when setting up replication. What do I do?

It is very possible you have multiple people using the `pgbelt` tool to set up replication. The config's `pglogical` password may be differnt in each person's config, and that is used during the `setup` stage. The password from the config is used to create the `pglogical` role in your databases.

Therefore, the first person to run `setup` has set the `pglogical` user's password in the databases. The error likely comes from `pglogical` mentioning a `node` configuration, where the password is set.

For information on `nodes` in the `pglogical` plugin, please see the `Extended Knowledge` document in this repository.

To remedy this issue, you can perform the following:

1. If you see the error with the entire DSN (including password and IP address or hostname), identify if the host is the **source** or **destination** database.
2. Once identified, run the following to PSQL into that host: `psql "$(belt <src/dst>-dsn <datacenter-name> <database-name>)"`
3. In that PSQL terminal, run the following to set the password according to the `node` configuration: `ALTER ROLE pglogical PASSWORD '<password-you-saw>';`

## How can I roll back?

**NOTE: The rollback process is not fully implemented in pgbelt. You should make every effort to solve
issues that surface only after writes have succeeded in the target database at the application level first!**

If you discover an application issue that requires a rollback to the old database, you can do so without data loss even after
writes have succeeded in the target database.

To perform a rollback you will need to begin another period of application downtime where neither
database receives any writes. Once you are sure downtime has begun, run the following:

    $ belt teardown-back-replication testdatacenter1 database1
    $ belt restore-logins testdatacenter1 database1

If you've lost the pgbelt config file where these users' names were stored when you ran the revoke logins
command, some users might be missed here.

Things that will need manual resolution:

- Sequence values on the source database. You will need to copy these over from the target database, no `belt` commands cover this yet.
- Tables without Primary Keys will need to be updated. You will need to copy these over from the target database to the source, no `belt` commands cover this yet.

After you are sure that sequences and tables without primary keys have been synchronized from the target
into the old source, point your application to the old source and your rollback is complete.

## I started a pgbelt replication job and need to restart it from scratch. How can I restart a pgbelt migration?

The following is a general guide to restarting a pgbelt migration. This is useful if you have a failed migration, or if you need to restart a migration after a rollback.

Run the following commands:

    $ belt teardown-back-replication testdatacenter1 database1
    $ belt teardown-forward-replication testdatacenter1 database1
    $ belt teardown testdatacenter1 database1
    $ belt teardown testdatacenter1 database1 --full
    $ belt remove-constraints testdatacenter1 database1
    $ belt remove-indexes testdatacenter1 database1

Note that the first four commands will remove all replication job setup from the databases. `remove-constraints` removes NOT VALID constraints from the target schema so when you restart replication, they don't cause failed inserts (these must not exist during the initial setup). `remove-indexes` removes all indexes from the target schema to help speed up the initial bulk load. `remove-indexes` is not necessary to run, you may skip this if needed.

After running these commands, you can `TRUNCATE` the tables in the destination database and start the migration from the beginning. **Please take as much precaution as possible when running TRUNCATE, as it will delete all data in the tables. Especially please ensure you are running this on the correct database!**

## My `sync` command has failed or is hanging. What can I do?

The `sync` command from Step 7 of the Quickstart guide does the following:

- Sync sequence values
- Dump and load tables without Primary Keys
- Add NOT VALID constraints to the target schema (they were removed in Step 1 in the target database)
- Create Indexes (as long as this was run in Step 2, this will be glossed over. If step 2 was missed, indexes will build now amd this will take longer than expected).
- Validate data (take 100 random rows and 100 last rows of each table, and compare data)
- Run ANALYZE to ensure optimal performance

If the `sync` command fails, you can try to run the individual commands that make up the `sync` command to see where the failure is. The individual commands are:

### 1. Syncing Sequences:

- `sync-sequences` - reads and sets sequences values from SRC to DST at the time of command execution

### 2. Syncing Tables without Primary Keys:

- `dump-tables` - dumps only tables without Primary Keys (to ensure only tables without Primary Keys are dumped, DO NOT specify the `--tables` flag for this command)
- `load-tables` - load into DST DB the tables from the `dump-tables` command (found on disk)

### 3. Syncing NOT VALID Constraints:

- `dump-schema` - dumps schema from your SRC DB schema onto disk (the files may already be on disk, but run this command just to ensure they exist anyways)
- `load-constraints` - load NOT VALID constraints from disk (obtained by the `dump-schema` command) to your DST DB schema

### 4. Creating Indexes & Running ANALYZE:

- `create-indexes` - Create indexes on the target database, and then runs ANALYZE as well.

### 5. Validating Data:

- `validate-data` - Check random 100 rows and last 100 rows of every table involved in the replication job, and ensure all match exactly.

## belt hangs when running `teardown --full`. What can I do?

If `belt` hangs when running `teardown --full`, it is likely having trouble dropping the `pglogical` extension. This normally happens due to any _idle in transaction_ connections to the database. To resolve this, you can run the following when it hangs:

- CTRL+C to stop the `teardown --full` command
- Identify which database is getting traffic (SRC or DST)
- List out the active connections and find which are _idle in transaction_:
  - `SELECT * FROM pg_stat_activity;`
- For each _idle in transaction_ connection, run the following:
  - `SELECT pg_terminate_backend(<pid>);`
- Once all _idle in transaction_ connections are terminated, you can run the `teardown --full` command again.
