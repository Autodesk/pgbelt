# Playbook

## I see an incorrect credential error with the `pglogical` user when setting up replication. What do I do?

It is very possible you have multiple people using the `pgbelt` tool to set up replication. The config's `pglogical` password is randomly generated in each person's config, and that is used during the `setup` stage. The password from the config is used to create the `pglogical` role in your databases.

Therefore, the first person to run `setup` has set the `pglogical` user's password in the databases. The error likely comes from `pglogical` mentioning a `node` configuration, where the password is set.

For information on `nodes` in the `pglogical` plugin, please see the `Extended Knowledge` document in this repository.

To remedy this issue, you can perform the following:

1. If you see the error with the entire DSN (including password and IP address or hostname), identify if the host is the **source** or **destination** database.
2. Once identified, run the following to PSQL into that host: `psql "$(belt <src/dst>-dsn <datacenter-name> <database-name>)"`
3. In that PSQL terminal, run the following to set the password according to the `node` configuration: `ALTER ROLE pglogical PASSWORD '<password-you-saw>';`
