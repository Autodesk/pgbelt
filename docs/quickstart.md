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
  // Optional key: "schema_name": "<someschema>". If the key isn't specified, the default will be "public". Schema name must be the same in source and destination DBs.
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
- All data to be migrated must be owned by a single login user, and that user must have CREATE permissions to create objects.
- All targeted data must live in the same schema in both the source and destination DBs.
- There must be a postgres superuser with a login in the database.
- Have the following parameters:
  - `max_replication_slots` >= 2 (at least 2 for use by this tool, add more if other tools are using slots as well)
  - `max_worker_processes` >= 2 (should be as high as your CPU count)
  - `max_wal_senders` >= 10 (Postgres default is 10, should not be lower than this)
  - `shared_preload_libraries` must include both `pg_stat_statements` and `pglogical`. _NOTE:_ You must ensure your destination database has all required extensions for your schema.
  - If your db is on AWS RDS you must also set `rds.logical_replication = 1`

# Migration Steps

## Step 1: Setup and Start Replication

This command will set up the target database's schema, pglogical and start replication from the
source to the destination.

    $ belt setup testdatacenter1 database1

You can check the status of the migration, database hosts, replication delay, etc using the following command:

    $ belt status testdatacenter1

## Step 2: Create Indexes on the target database before your application cutover

To ensure the bulk COPY phase of the migration runs faster, indexes are not made in the destination database during setup.
They need to be built and this process should be done before the cutover to not prolong your cutover window. You should run
this command during a period of low traffic.

Note that this command will create all the indexes in the target database, **and will run ANALYZE after** to ensure optimal performance.

    $ belt create-indexes testdatacenter1 database1

## Step 3: (Optional) Run ANALYZE on the target database before your application cutover

This is typically run some time before your application cutover, so the target database performs better with the dataset
once the application cuts over to the target database.

    $ belt analyze testdatacenter1 database1

## Step 4: Set up Reverse Replication

We will set up reverse replication (destination to source), in case rollback is needed later.

This can be done at any time before the next step, but for clarity, we will do it now. It also is a very quick operation. Doing this later also eliminates any possibility of bad writes on the destination database being replicated back to the source.

    $ belt setup-back-replication testdatacenter1 database1

## Step 5: Stop write traffic to your source database

This would be the beginning of your application downtime. We revoke all login permissions on the source host using `belt` to ensure writes can no longer occur. You may want to do this, then restart Postgres connections on your application to ensure connections can no longer write.

**NOTE: Do not run this command if the schema owner of your database is the same as your root user.**

    $ belt revoke-logins testdatacenter1 database1

## Step 6: Stop forward replication

Once write traffic has stopped on the source database, we need to stop replication in the forward direction.

    $ belt teardown-forward-replication testdatacenter1 database1

## Step 7: Sync all the missing bits from source to destination (that could not be done by replication)

PgLogical (used for the actual replication) can't handle the following:

- Replicating Sequences (see https://github.com/2ndQuadrant/pglogical/issues/163)
- Replicating tables without Primary Keys
- Replicate data with NOT VALID constraints into the target schema (since by nature, they are only enforced in a dataset once applied, not for all previous records)

Tables without primary keys were already ignored as part of Step 1, and NOT VALID constraints were removed when the schema was set up in the target database in Step 1.

Therefore the next command will do the following:

- Sync sequence values
- Dump and load tables without Primary Keys
- Add NOT VALID constraints to the target schema (they were removed in Step 1 in the target database)
- Create Indexes (as long as this was run in Step 2, this will be glossed over. If step 2 was missed, indexes will build now amd this will take longer than expected).
- Validate data (take 100 random rows and 100 last rows of each table, and compare data)
- Run ANALYZE to ensure optimal performance

```
$ belt sync testdatacenter1 database1
```

If the above command fails, please see the `playbook.md` document in this repository for more information on how to resolve the issue.

## Step 8: Enable write traffic to the destination host

Enabling write traffic to the destination host is done outside of PgBelt, with your application.

## Step 9: Teardown pgbelt replication and leftover objects!

Up until this step, reverse replication will be ongoing. It is meant to do this until you feel a rollback is unnecessary. To stop reverse replication, and consider your pgbelt migration **complete**, simply run the following:

    $ belt teardown testdatacenter1 database1
    $ belt teardown testdatacenter1 database1 --full

The first command will tear down all replication jobs if still running. At this point, you should only have your reverse replication running. It will also tear down all of the pgbelt replication job objects in the database, including the `pglogical` role used by the jobs.

The second command will run through the first command, and finally drop the `pglogical` extension from the database. This is separated out because the extension drop tends to hang if the previous steps are done right beforehand. When run separately, the DROP command likely will run without hanging or run in significantly less time.

# Final Notes

Please note that instructions for rolling back and restarting a migration are now in the playbook in this directory. Please refer to those for more information.
