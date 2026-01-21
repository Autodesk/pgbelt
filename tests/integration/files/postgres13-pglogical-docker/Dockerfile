
FROM postgres:13

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
  postgresql-server-dev-13

ARG PGLOGICAL_TAG=REL2_4_4

RUN git clone --depth 1 --branch ${PGLOGICAL_TAG} https://github.com/2ndQuadrant/pglogical.git /tmp/pglogical \
  && make -C /tmp/pglogical \
  && make -C /tmp/pglogical install \
  && rm -rf /tmp/pglogical

# Configure with settings pgbelt requires (max_wal_senders, max_replication_slots, max_worker_processes, shared_preloaded_libraries)
RUN echo "host    replication          postgres                172.18.0.0/16   trust" >> /usr/share/postgresql/13/pg_hba.conf.sample
RUN echo "host    replication          postgres                ::1/128         trust" >> /usr/share/postgresql/13/pg_hba.conf.sample
RUN echo "shared_preload_libraries = 'pg_stat_statements,pglogical'" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "wal_level = 'logical'" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "max_wal_senders = 20" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "max_replication_slots = 20" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "max_worker_processes = 20" >> /usr/share/postgresql/postgresql.conf.sample
