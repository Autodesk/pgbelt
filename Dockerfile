FROM python:3.9-slim
COPY ./ /opt/pgbelt
WORKDIR /opt/pgbelt

RUN set -e \
    && python -m pip install --upgrade pip \
    && pip install poetry poetry-dynamic-versioning \
    && poetry install

RUN set -e \
    && apt-get -y update \
    && apt-get -y install postgresql-client
