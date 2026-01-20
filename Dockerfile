FROM python:3.13-slim
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN set -e \
    && apt-get -y update

RUN apt-get -y install \
    postgresql-client \
    gcc

RUN set -e \
    && python -m venv $VIRTUAL_ENV \
    && python -m pip install --upgrade pip \
    && pip install poetry poetry-dynamic-versioning

COPY ./ /opt/pgbelt
WORKDIR /opt/pgbelt
RUN poetry install
