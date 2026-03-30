ARG PYTHON_VERSION=3.13
FROM python:${PYTHON_VERSION}-slim
ENV VIRTUAL_ENV=/opt/venv
ENV POETRY_HOME=/opt/poetry
ENV PATH="$POETRY_HOME/bin:$VIRTUAL_ENV/bin:$PATH"

RUN set -e \
    && apt-get -y update

RUN apt-get -y install \
    postgresql-client \
    gcc

RUN set -e \
    && python -m venv $POETRY_HOME \
    && $POETRY_HOME/bin/pip install poetry poetry-dynamic-versioning

RUN python -m venv $VIRTUAL_ENV

COPY ./ /opt/pgbelt
WORKDIR /opt/pgbelt
RUN pip install --upgrade pip \
    && poetry install
