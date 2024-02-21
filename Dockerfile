FROM python:3.11-slim

RUN set -e \
    && apt-get -y update \
    && apt-get -y install postgresql-client \
    && apt-get -y install gcc

RUN set -e \
    && python -m pip install --upgrade pip \
    && pip install poetry poetry-dynamic-versioning
