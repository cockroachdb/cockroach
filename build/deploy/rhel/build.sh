#!/usr/bin/env bash

set -ux

# Temporarily copy the necessary files into this directory (and clean
# them up afterwards) because docker build can't access resources
# from parent directories.
cp ../../../LICENSE ../../../APL.txt ../cockroach.sh ../cockroach ./
docker build -t cockroachdb:${VERSION} .
rm LICENSE APL.txt cockroach.sh cockroach
