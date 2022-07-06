# Configuring Pgbelt
## Local Configuration Files
You will need to provide pgbelt with credentials it can use to log in to your databases.
Configuration lives in json files in directories relative to the working directory when you
run pgbelt commands: `./configs/$DATACENTER/$DATABASE/config.json`

Each file contains configuration for one pair of databases, a source and a target.
Files are logically grouped under a datacenter name. The datacenter and database names are
used to refer to specific config files or groups of config files when executing pgbelt commands.

**Be very careful never to store these files in source control or paste them into chat applications!
They will contain database login credentials!**

### Example
Here's an example config file for a database in `dev` named `mydatabase`.
This file is located at `./configs/dev/mydatabase/config.json`

```json
{
    "db": "mydatabase",
    "dc": "dev",
    "src": {
        "host": "mydatabase.example.com",
        "ip": "10.235.87.141",
        "db": "mydatabasename",
        "port": "5432",
        "root_user": {
            "name": "postgres",
            "pw": "[REDACTED]"
        },
        "owner_user": {
            "name": "owner",
            "pw": "[REDACTED]"
        },
        "pglogical_user": {
            "name": "pglogical",
            "pw": "[REDACTED]"
        },
        "other_users": [
            {
                "name": "someotheruser",
                "pw": "[REDACTED]"
            },
            {
                "name": "anotheruser",
                "pw": null
            }
        ]
    },
    "dst": {
        "host": "mynewdatabase.example.com",
        "ip": "10.235.110.129",
        "db": "mydatabasename",
        "port": "5432",
        "root_user": {
            "name": "postgres",
            "pw": "[REDACTED]"
        },
        "owner_user": {
            "name": "owner",
            "pw": "[REDACTED]"
        },
        "pglogical_user": {
            "name": "pglogical",
            "pw": "[REDACTED]"
        },
        "other_users": null
    }
}
```

### Fields
* `"db"` the name of the directory containing the config file.
* `"dc"` the name of the directory one level higher.
* `"src"` an object describing the source database instance. May be set null
    * `"host"` a hostname pointing to the source database instance.
    * `"ip"` the vpc internal ip of the source database instance
    * `"db"` the name of the database within the instance you want to migrate.
    * `"port"` the port used to connect to the source database instance.
    * `"root_user"` an object describing a superuser in the source db. This user must be able to create roles and extensions.
        * `"name"` this user's username
        * `"pw"` this user's password
    * `"owner_user"` a user in the source db that owns all the tables and sequences to be replicated.
    * `"pglogical_user"` a user that pgbelt will create in the source db to be used with pglogical.
    * `"other_users"` a list of other users in the source db. Users in this list may have null passwords. May be set null.
* `"dst"` same as src but describes the destination database instance. May be set null

Some commands require only the src or dst field to be filled in. Check usage.md or
the help for each command to see if this is the case.
