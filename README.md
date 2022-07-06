# Pgbelt

Pgbelt is a tool to assist in migrating data between postgres databases with as
little application downtime as possible. It works in databases running different versions
of postgres and makes it easy to run many migrations in parallel during a single downtime.


## What Does It Do?
Pgbelt automates all the database operations required to perform a data migration between
two postgres databases. This includes:

* Loading your target database with the schema from the source database.
* Managing continuous data replication from the source database to the target.
* Dumping and loading data that can not be replicated continuously.
* Verifying data integrity between your databases.
* Managing data replication in the opposite direction to allow rollbacks after writes have occurred in the target.
* Managing migrations of many databases at once so N databases can be moved during one downtime.


## What Doesn't It Do?
After a migration is performed using pgbelt, data in the source and target database
will match exactly even if it can not be replicated using pglogical. In order to
make this happen there are some limitations:

* Continuous replication is limited to tables with primary keys only, and only between two databases.
* Sequences are not replicated continuously with pglogical.
* You must stop writes to the source to synchronize data that cannot be replicated continuously.
* You may not allow writes to both the source and target simultaneously.

There are some additional limitations as well which are unrelated. These may change in the future:

* Pgbelt does not automate synchronizing data other than tables with primary keys during rollbacks.
* Pgbelt does not modify postgres parameters.
* Pgbelt assumes all data that needs to be moved is in the public schema.
* All data in the target database will be owned by a single user.


## Database Requirements
Both your source and target database must satisfy the following requirements:
* Be running postgreSQL version 9.6 or greater.
* Each database must be accessible from the other on the network.
* All data to be migrated must be in the public schema.
* All data to be migrated must be owned by a single login user.
* There must be a postgres superuser with a login in the database.
* Have the following parameters:
    * max_replication_slots >= 20
    * max_worker_processes >= 20
    * max_wal_senders >= 20
    * shared_preload_libraries must include both pg_stat_statements and pglogical
    * If your db is on AWS RDS you must also set rds.logical_replication = 1

Pgbelt includes a utility to check whether your database satisfies these requirements.
See the Checking Source DB Prerequisites section below.

## Installation
### Using Brew
If you are on a mac this is the best way to install pgbelt. You must have read
access to this repo and [brew](https://brew.sh/) installed to use this method.

    # install pgbelt with brew
    brew tap autodesk/pgbelt git@github.com:Autodesk/pgbelt.git
    brew install pgbelt

    # check that belt commands work
    belt --help

    # to update belt later
    brew upgrade pgbelt

### Install From A Clone
If you can't use brew but are proficient with python and virtual environments
you can install pgbelt as an egg from a local clone of this repo. These instructions
assume you are using [pyenv](https://github.com/pyenv/pyenv) and the
[pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv) plugin to manage
python versions and virtual environments.

If you use this installation method you will have to manually install postgres
client tools like pg_dump and pg_restore that belt depends on.

    # create a python virtualenv with python 3.9.6 and activate it (any 3.9.x is ok)
    pyenv install 3.9.6
    pyenv virtualenv 3.9.6 pgbelt
    pyenv activate pgbelt

    # clone the repo
    git clone git@github.com:Autodesk/pgbelt.git
    cd pgbelt

    # install pgbelt in your virtualenv
    make install -e .

    # check that belt commands work
    belt --help

    # check that you have required postgres utilities installed
    pg_dump --help
    pg_restore --help


    # to update belt later
    git checkout master
    git pull

If you use this installation method then you will only be able to run belt
commands when this virtual environment is activated. You will also be running
the code you have checked out in your local clone of the pgbelt repo.

## Using Pgbelt

| :exclamation:  This is very important   |
|:-----------------------------------------|
| As with all Data Migration tasks, **there is a risk of data loss**. Please ensure you have backed up your data before attempting any migrations |

You will need a directory to store your belt configuration in. You will always
run pgbelt from this directory.

You will need at least a `configs` folder in this directory. If you are using
a python virtualenv, you may want to create it in this folder and store
requirements here too.

You will need to provide configuration files for belt as described
[here](#configuring-pgbelt) before running any belt commands.
All commands follow the structure `belt COMMAND DATACENTER DATABASE`.
You can run almost any command on all the databases in a datacenter concurrently
by simply omitting the database name from the command like `belt status DATACENTER`.

For more details on usage check docs/usage.md or the built in command help.


## Configuring Pgbelt
You will need to provide pgbelt with credentials to log in to your databases.
Configuration lives in json files in directories relative to the working directory where you
run pgbelt commands. If you cloned the `pgbelt-migration-template` repo, these directories are
already set up there. They are the `configs` and `remote-configs` directories.

Each file contains configuration for one pair of databases, a source and a target.
Files are logically grouped under a datacenter name. The datacenter and database names are
used to refer to specific config files or groups of config files when executing pgbelt commands.

The following examples will be for a database `mydatabase` in the environment `dev`.

### Local Configuration
These are the files stored under the `configs` directory. They contain actual secret values.
The configuration file for our example db `mydatabse` in `dev` would be `./configs/dev/mydatabase/config.json`.
When you run a belt command like `belt precheck dev mydatabase` it will get any secrets it needs to log in
from this file. **Belt will always try to find configuration files in a directory relative to the current working
directory when you execute the command.** This is why you need a project directory like the
pgbelt-migration-template.

See docs/config.md for details on how these files are structured and how to write them.

**Be very careful never to store these files in source control or paste them into chat applications!
They will contain database login credentials!**

### Remote Configuration
Because it can be cumbersome to share files full of secrets between members of a team,
you may also define remote configuration files which live under the `remote-configs` directory.
The configuration file for our example db `mydatabse` in `dev` would be `./remote-configs/dev/mydatabase/config.json`.

These files are meant to contain no actual secrets. Instead they contain information
about where the secrets are stored remotely. This makes them much easier to share as they can be checked into git.

Currently this only works for databases managed by Dacloud, but pgbelt allows you to
implement your own remote config resolver and use it as a plugin. If a
remote config file is present, belt will pass its contents to the resolver which generates
a local config file.

For more details on remote configuration see docs/config.md


## Checking Source DB Prerequisites
Pgbelt includes a utility to check whether your database satisfies its requirements.
For this command to run you must first provide a configuration file and set up a directory
in which you can run belt as described above.
Only the source db configuration in the file needs to be valid.

Once you have belt installed and a configuration file created run

    $ belt precheck dev mydatabase

to see whether belt will work for your database. This will also produce
a detailed view of which tables and sequences can be replicated with pgbelt.

**Note: This command does not check the target database configuration or check
for network connectivity between the two databases.**


## Migration Guide
This guide will describe a typical migration process using belt
for a single database `mydatabase` in the datacenter `dev`.

### Before Migration Day
You can start continuous replication from the source to the target any time
before you need to perform the actual cutover. Make sure to leave enough
time for the initial preload to complete. How long this takes depends on how
much data is in your database.
We recommend that you test how long this takes with a clone of the largest
database you want to migrate and plan accordingly.

    $ belt setup dev mydatabase

This command configures pglogical in the source and target database,
loads the schema from the source into the target, and starts
replicating all data from the source to the target.
**Note: Only tables with primary keys can be replicated this way.**

Pglogical will perform an initial load of all data present in the source, then transition
to replicating only write operations. You can check what's happening by running

    $ belt status dev mydatabase

When the status is `initializing` the preload is still in progress. Once it's over
the status will change to `replicating`. Once the status is `replicating` you may
turn on back replication from the target to the source:

    $ belt setup-back-replication dev mydatabase

This won't do anything until you actually perform the cutover and your application begins writing
to the new database. Having back replication set up before this happens ensures that you will not lose
data if you need to roll back after cutting over. Any successful writes in the target will be present
in the source after you roll back.

### Downtime and Cutover
Pgbelt requires downtime to synchronize data that can not be moved with pglogical.
Downtime means that nothing may write to either the source or the target database.
It is mostly up to you to ensure that this requirement is met. To help prevent writes
to the source, you can use the command

    $ belt revoke-logins dev mydatabase

to revoke login permissions from all users in the source. **This does not end connections that
were already in progress.** It is up to you to make sure everything is disconnected.
An easy way to do this is to restart any applications that might be connected.

When you are sure that nothing is writing to either database, run the following:

    $ belt teardown-forward-replication dev mydatabase
    $ belt sync dev mydatabase

to stop pglogical replication from the source to the target and
synchronize all data that can not be replicated with pglogical. This includes
sequences and all tables that do not have primary keys. How long this takes depends
on the volume of data in tables without primary keys.
We recommend that you test how long this takes with a clone of the largest
database you want to migrate and plan accordingly.
This command will also verify that the data in the source matches the data in
the target and run ANALYZE in the target.

If the sync command completes successfully, then the target database is ready and you are
free to allow your application to write into it.


## Rolling Back
**NOTE: The rollback process is not fully implemented in pgbelt. You should make every effort to solve
issues that surface only after writes have succeeded in the target database at the application level first!**

If you follow the migration plan above, your target database will still be configured to replicate
all writes back into the old source you migrated away from. If you discover an application
issue that requires a rollback to the old database you can do so without data loss even after
writes have succeeded in the target database.

To perform a rollback you will need to begin another period of application downtime where neither
database receives any writes. Once you are sure downtime has begun, run the following:

    $ belt teardown-back-replication dev mydatabase
    $ belt restore-logins dev mydatabase

This will stop replication from your target to the old source and restore login permissions to any
users in the source from which it was revoked earlier. If you've lost the pgbelt config file where
these users' names were stored when you ran the revoke command then some users might be missed here.

It is up to you to update the sequence values in the source to match those in the target. If you don't,
then you will see data conflict errors when your application attempts to write into the source again.
The same is true for any tables that do not contain primary keys.

After you are sure that sequences and tables without primary keys have been synchronized from the target
into the old source, point your application to the old source and your rollback is complete.
