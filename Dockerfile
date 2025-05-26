FROM golang:1.23 AS pgcompare-builder

WORKDIR /opt/pgcompare

COPY ./pg-compare /opt/pgcompare

RUN set -e \
    && go mod download \
    && for GOOS in linux darwin windows; do \
        for GOARCH in amd64 arm64; do \
            EXT="so"; \
            [ "$GOOS" = "windows" ] && EXT="dll"; \
            go build -o "pgcompare_${GOOS}_${GOARCH}.${EXT}" -buildmode=c-shared main.go; \
        done; \
    done

FROM python:3.11-slim
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
COPY ./ /opt/pgbelt
WORKDIR /opt/pgbelt

COPY --from=pgcompare-builder /opt/pgcompare/pgcompare_linux_amd64.so /opt/pgbelt/pgcompare_linux_amd64.so
COPY --from=pgcompare-builder /opt/pgcompare/pgcompare_linux_arm64.so /opt/pgbelt/pgcompare_linux_arm64.so
COPY --from=pgcompare-builder /opt/pgcompare/pgcompare_darwin_amd64.so /opt/pgbelt/pgcompare_darwin_amd64.so
COPY --from=pgcompare-builder /opt/pgcompare/pgcompare_darwin_arm64.so /opt/pgbelt/pgcompare_darwin_arm64.so
COPY --from=pgcompare-builder /opt/pgcompare/pgcompare_windows_amd64.dll /opt/pgbelt/pgcompare_windows_amd64.dll
COPY --from=pgcompare-builder /opt/pgcompare/pgcompare_windows_arm64.dll /opt/pgbelt/pgcompare_windows_arm64.dll

RUN set -e \
    && apt-get -y update \
    && apt-get -y install postgresql-client \
    && apt-get -y install gcc

RUN set -e \
    && python -m venv $VIRTUAL_ENV \
    && python -m pip install --upgrade pip \
    && pip install poetry poetry-dynamic-versioning \
    && poetry install
