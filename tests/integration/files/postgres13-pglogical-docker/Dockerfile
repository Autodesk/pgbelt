
FROM postgres:13

ARG PGLOGICAL_VERSION=2.4.3

# Install pglogical onto the image
RUN apt-get update && apt-get install -y wget gnupg
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ bullseye-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
  && wget --quiet -O /usr/share/keyrings/postgresql.gpg https://www.postgresql.org/media/keys/ACCC4CF8.asc \
  && echo "deb [signed-by=/usr/share/keyrings/postgresql.gpg] http://apt.postgresql.org/pub/repos/apt/ bullseye-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
  && apt-get update \
  && PGLOGICAL_PKG_VERSION=$(apt-cache madison postgresql-13-pglogical | awk -v v="${PGLOGICAL_VERSION}" '$3 ~ v {print $3; exit}') \
  && if [ -z "$PGLOGICAL_PKG_VERSION" ]; then echo "pglogical ${PGLOGICAL_VERSION} not found"; exit 1; fi \
  && apt-get install -y postgresql-13-pglogical=${PGLOGICAL_PKG_VERSION}

# Configure with settings pgbelt requires (max_wal_senders, max_replication_slots, max_worker_processes, shared_preloaded_libraries)
RUN echo "host    replication          postgres                172.18.0.0/16   trust" >> /usr/share/postgresql/13/pg_hba.conf.sample
RUN echo "host    replication          postgres                ::1/128         trust" >> /usr/share/postgresql/13/pg_hba.conf.sample
RUN echo "shared_preload_libraries = 'pg_stat_statements,pglogical'" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "wal_level = 'logical'" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "max_wal_senders = 20" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "max_replication_slots = 20" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "max_worker_processes = 20" >> /usr/share/postgresql/postgresql.conf.sample
