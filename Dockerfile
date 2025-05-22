FROM golang:1.23 as pgcompare-builder

WORKDIR /opt/pgcompare

COPY ./pg-compare /opt/pgcompare

RUN set -e \
    && go mod download \
    && GOOS=linux GOARCH=amd64 go build -o pg-compare-cli-linux ./pg-compare-linux \
    && GOOS=darwin GOARCH=amd64 go build -o pg-compare-cli-macos ./pg-compare-macos \
    && GOOS=windows GOARCH=amd64 go build -o pg-compare-cli-windows.exe ./pg-compare-windows.exe

FROM python:3.11-slim
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
COPY ./ /opt/pgbelt
WORKDIR /opt/pgbelt

COPY --from=pgcompare-builder /opt/pgcompare/pg-compare-linux .
COPY --from=pgcompare-builder /opt/pgcompare/pg-compare-macos .
COPY --from=pgcompare-builder /opt/pgcompare/pg-compare-windows.exe .

RUN set -e \
    && apt-get -y update \
    && apt-get -y install postgresql-client \
    && apt-get -y install gcc

RUN set -e \
    && python -m venv $VIRTUAL_ENV \
    && python -m pip install --upgrade pip \
    && pip install poetry poetry-dynamic-versioning \
    && poetry install
