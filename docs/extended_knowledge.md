# Extended Knowledge with `pgbelt`

## How `pglogical` replication works

### How a replication task works logically

- We have a replication task that runs in **two phases**:
  1. Full Load / Bulk Sync. Moving the majority of data takes a lot of time, so it is all dumped and loaded **at a specific timestamp**. While this occurs, any ongoing changes to the dataset from that timestamp onwards are stored in a **replication slot**.
  2. Once the above step is finished, ongoing changes are consumed from the source database's replication slot and replayed on the destination database. This is an ongoing process.

### Pglogical Components for a Replication task

- Node - A way of telling pglogical the existence of an external database, along with the credentials to connect with.
- Subscription - A replication task initiated from the side of the subcribing node, or destination database.
- Replication Set - A set of tables to replicate, along with settings of what action/statement types to replicate.
  - We replicate **all** actions, but the list of tables to replicate may vary. We replicate all tables in a database major version upgrade, but also only do subsets for "exodus-style" migrations.

### What `pgbelt` does with the above components:

- Configure the pglogical nodes for the external database in both the source and destination databases.
- For forward replication (source to destination)
  - Create a new replication set in the source DB, and add all required tables to it.
  - Start a new subscription from the destination DB, referencing the above replication set.
- For reverse replication (destination to source)
  - Create a new replication set in the destination DB, and add all required tables to it.
  - Start a new subscription from the source DB, referencing the above replication set, **and with synchronize_structure off**.
    - The last flag ensures no full load sync occurs from the destination DB (incomplete/empty) to the source database. It will only replicate transactions other than the incoming forward replication statements.
