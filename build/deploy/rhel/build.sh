#!/usr/bin/env bash

set -ux

# Temporarily copy the necessary files into this directory (and clean
# them up afterwards) because docker build can't access resources
# from parent directories.
cp ../cockroach.sh ../cockroach ./
mkdir licenses
cp -r ../../../LICENSE ../../../licenses/ ./licenses
docker build --no-cache --pull -t cockroachdb:${VERSION} .
rm -r licenses cockroach.sh cockroach
