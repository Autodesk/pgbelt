
FROM postgres:16

# Build pglogical from source to pin to a specific release tag.
RUN apt-get update && apt-get install -y \
  ca-certificates \
  git \
  make \
  gcc \
  libkrb5-dev \
  libselinux1-dev \
  libxslt1-dev \
  libpam0g-dev \
  zlib1g-dev \
  liblz4-dev \
  libzstd-dev \
  postgresql-16 \
  postgresql-server-dev-16

ARG PGLOGICAL_TAG=REL2_4_6

RUN git clone --depth 1 --branch ${PGLOGICAL_TAG} https://github.com/2ndQuadrant/pglogical.git /tmp/pglogical \
  && make -C /tmp/pglogical PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config LDFLAGS="-L/usr/lib/postgresql/16/lib" \
  && make -C /tmp/pglogical PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config LDFLAGS="-L/usr/lib/postgresql/16/lib" install \
  && rm -rf /tmp/pglogical

# Configure with settings pgbelt requires (max_wal_senders, max_replication_slots, max_worker_processes, shared_preloaded_libraries)
RUN echo "host    replication          postgres                172.18.0.0/16   trust" >> /usr/share/postgresql/16/pg_hba.conf.sample
RUN echo "host    replication          postgres                ::1/128         trust" >> /usr/share/postgresql/16/pg_hba.conf.sample
RUN echo "shared_preload_libraries = 'pg_stat_statements,pglogical'" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "wal_level = 'logical'" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "max_wal_senders = 20" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "max_replication_slots = 20" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "max_worker_processes = 20" >> /usr/share/postgresql/postgresql.conf.sample
# PostgreSQL 16.15+ (CVE-2026-6471) only allows trusted logical output plugins,
# defaulting to 'pgoutput, test_decoding'. pglogical needs pglogical_output.
RUN echo "output_plugin_libraries = 'pgoutput, test_decoding, pglogical_output'" >> /usr/share/postgresql/postgresql.conf.sample

# Test extension that owns tables, like PostGIS (spatial_ref_sys) and Aurora
# apg_plan_mgmt (apg_plan_mgmt.plans). Used to test that pgbelt copies
# extension-owned tables instead of replicating them.
#   - pgbelt_ext_config: a config table in the extension's schema, with stock
#     rows that its pg_extension_config_dump filter excludes. Like PostGIS,
#     the extension grants SELECT on it to PUBLIC.
#   - pgbelt_ext_data.plans: a table that isn't a config table, in a schema
#     the extension creates.
RUN printf "%s\n" \
  "comment = 'pgbelt integration test extension that owns tables'" \
  "default_version = '1.0'" \
  "relocatable = false" \
  "superuser = true" \
  > /usr/share/postgresql/16/extension/pgbelt_test_ext.control \
  && printf "%s\n" \
  "CREATE TABLE @extschema@.pgbelt_ext_config (id int PRIMARY KEY, val text NOT NULL);" \
  "INSERT INTO @extschema@.pgbelt_ext_config VALUES (1, 'stock'), (2, 'stock');" \
  "SELECT pg_catalog.pg_extension_config_dump('@extschema@.pgbelt_ext_config', 'WHERE id > 100');" \
  "GRANT SELECT ON @extschema@.pgbelt_ext_config TO PUBLIC;" \
  "CREATE SCHEMA pgbelt_ext_data;" \
  "CREATE TABLE pgbelt_ext_data.plans (plan_id int PRIMARY KEY, status text NOT NULL);" \
  > /usr/share/postgresql/16/extension/pgbelt_test_ext--1.0.sql
