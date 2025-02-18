
FROM postgres:13

# Install pglogical onto the image
RUN apt-get update && apt-get install -y wget gnupg
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ bullseye-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
  && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | \
       apt-key add - \
  && apt-get update \
  && apt-get install -y postgresql-13-pglogical

# Configure with settings pgbelt requires (max_wal_senders, max_replication_slots, max_worker_processes, shared_preloaded_libraries)
RUN echo "host    replication          postgres                172.18.0.0/16   trust" >> /usr/share/postgresql/13/pg_hba.conf.sample
RUN echo "host    replication          postgres                ::1/128         trust" >> /usr/share/postgresql/13/pg_hba.conf.sample
RUN echo "shared_preload_libraries = 'pg_stat_statements,pglogical'" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "wal_level = 'logical'" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "max_wal_senders = 20" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "max_replication_slots = 20" >> /usr/share/postgresql/postgresql.conf.sample
RUN echo "max_worker_processes = 20" >> /usr/share/postgresql/postgresql.conf.sample
