# Quickstart with PgBelt

The base case is using the tool to migrate all data from one Postgres database to another, while being used by an application.

Let's say we have `database1` inside datacenter `testdatacenter1`, and you are moving this database's data
from one host to another. The source host has all the data, the destination is **empty, not even a schema** (aside -- belt
can work with preloaded schemas in the destination, just not done in this example).

This example will set up **bidirectional replication** between your source and destination hosts. This is useful to have configured
so after cutover, your application can keep writing updates to the source in case a rollback to the source host is required. The
destination will only write/replicate back to the source once writes occur on the destination host.

# Local Setup

## Step 1: Install PgBelt on your machine

Pgbelt is now available on PyPi! Install it locally:

    pip3 install pgbelt

## Step 2: Create Required Folder and File Structure

Create a migration project folder as follows:

    mymigrationproject/
      configs/
        testdatacenter1/ # Datacenter names are at this level
          database1/ # Database names are at this level
            config.json

## Step 3: Set up configuration

Fill in config.json with the required info (marked in `<>`), referring to this example:

```json
{
  "db": "database1",
  "dc": "testdatacenter1",
  "src": {
    // Anything in here must match what is in the host
    "host": "<mydatabase1src.example.com>",
    "ip": "<###.###.###.###>",
    "db": "<internaldbname1src>",
    "port": "<####>",
    "root_user": {
      "name": "<root-username>",
      "pw": "<root-password>"
    },
    "owner_user": {
      "name": "<username-that-owns-all-the-tables>",
      "pw": "<password-for-that-user>"
    },
    "pglogical_user": {
      // PgBelt will use this info to create this user in your database.
      "name": "pglogical", // No need to change this
      "pw": "<fill-your-own-password>" // You can use the following: python3 -c "from string import ascii_letters; from string import digits; from random import choices; print(\"\".join(choices(ascii_letters + digits, k=16)))";
    },
    "other_users": null
  },
  "dst": {
    // Anything in here must match what is in the host
    "host": "<mydatabase1dst.example.com>",
    "ip": "<###.###.###.###>",
    "db": "<internaldbname1dest>",
    "port": "<####>",
    "root_user": {
      "name": "<root-username>",
      "pw": "<root-password>"
    },
    "owner_user": {
      "name": "<username-that-owns-all-the-tables>",
      "pw": "<password-for-that-user>"
    },
    "pglogical_user": {
      // PgBelt will use this info to create this user in your database.
      "name": "pglogical", // No need to change this
      "pw": "<fill-your-own-password>" // You can use the following: python3 -c "from string import ascii_letters; from string import digits; from random import choices; print(\"\".join(choices(ascii_letters + digits, k=16)))";
    },
    "other_users": null
  },
  "tables": [],
  "sequences": []
}
```

## Step 4: Confirm PgBelt can be used with your hosts

Run the `belt precheck` command to check if `belt` can work for your migration.
If any requirements fail, they show in red and need to be reconfigured on your
database.

NOTE: You must run `belt` from the root of the `mymigrationproject/` folder,
as `belt` will check for configs based on relative pathing from where it is run.

    $ belt precheck testdatacenter1

**Also note: this command does not check the target database configuration or check
for network connectivity between the two databases.**

### Database Requirements

Both your source and target database must satisfy the following requirements:

- Be running postgreSQL version 9.6 or greater.
- Each database must be accessible from the other on the network.
- All data to be migrated must be in the public schema.
- All data to be migrated must be owned by a single login user.
- There must be a postgres superuser with a login in the database.
- Have the following parameters:
  - `max_replication_slots` >= 20
  - `max_worker_processes` >= 20
  - `max_wal_senders` >= 20
  - `shared_preload_libraries` must include both `pg_stat_statements` and `pglogical`
  - If your db is on AWS RDS you must also set `rds.logical_replication = 1`

# Migration Steps

## Step 1: Setup and Start Replication

This command will set up the target database's schema, pglogical and start replication from the
source to the destination. We will also set up reverse replication (destination to source), in
case rollback is needed later.

    $ belt setup testdatacenter1 database1
    $ belt setup-back-replication testdatacenter1 database1

You can check the status of the migration, database hosts, replication delay, etc using the following command:

    $ belt status testdatacenter1

## Step 2: Run ANALYZE on the target database before your application cutover

This is typically run some time before your application cutover, so the target database performs better with the dataset
once the application cuts over to the target database.

    $ belt analyze testdatacenter1 database1

## Step 3: Stop write traffic to your source database

This would be the beginning of your application downtime. We revoke all login permissions on the source host using `belt` to ensure writes can no longer occur. You may want to do this, then restart Postgres connections on your application to ensure connections can no longer write.

    $ belt revoke-logins testdatacenter1 database1

## Step 4: Stop forward replication

Once write traffic has stopped on the source database, we need to stop replication in the forward direction.

    $ belt teardown-forward-replication testdatacenter1 database1

## Step 5: Sync all the missing bits from source to destination (that could not be done by replication)

PgLogical (used for the actual replication) can't handle the following:

- Replicating Sequences (see https://github.com/2ndQuadrant/pglogical/issues/163)
- Replicating tables without Primary Keys
- Replicate data with NOT VALID constraints into the target schema (since by nature, they are only enforced in a dataset once applied, not for all previous records)

Tables without primary keys were already ignored as part of Step 1, and NOT VALID constraints were removed when the schema was set up in the target database in Step 1.

Therefore the next command will do the following:

- Sync sequence values
- Dump and load tables without Primary Keys
- Add NOT VALID constraints to the target schema (they were removed in Step 1 in the target database)
- Validate data (take 100 random rows and 100 last rows of each table, and compare data)
- Run ANALYZE to ensure optimal performance

```
$ belt sync testdatacenter1 database1
```

## Step 6: Enable write traffic to the destination host

This is done outside of PgBelt, with your application. Note -- reverse replication will be ongoing until you feel a rollback is unnecessary. To stop reverse replication, simply run the following:

    $ belt teardown-back-replication testdatacenter1 database1

# (Optional) Rolling Back

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

- Sequence values on the source database
- Tables without Primary Keys will need to be updated

After you are sure that sequences and tables without primary keys have been synchronized from the target
into the old source, point your application to the old source and your rollback is complete.
