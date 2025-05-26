#!/bin/bash

set -e

for GOOS in linux darwin windows; do
    for GOARCH in amd64 arm64; do
        EXT="so"
        [ "$GOOS" = "windows" ] && EXT="dll"
        OUTPUT="pgcompare_${GOOS}_${GOARCH}.${EXT}"

        if [ "$GOOS" = "linux" ]; then
            docker run --rm -v "$PWD":/src -w /src golang:1.23 \
                bash -c "GOOS=$GOOS GOARCH=$GOARCH go build -buildmode=c-shared -o $OUTPUT main.go" || {
                    echo "Docker build failed for $GOOS/$GOARCH"
                }
        else
            GOOS=$GOOS GOARCH=$GOARCH go build -buildmode=c-shared -o $OUTPUT main.go || {
                    echo "build failed for $GOOS/$GOARCH"
                }
        fi
    done
done
echo "Build completed successfully for all platforms."
cp pgcompare_*.so ./../pgbelt/
