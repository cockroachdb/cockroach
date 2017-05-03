#!/usr/bin/env bash

set -euo pipefail

build/builder.sh make archive ARCHIVE=build/cockroach.src.tgz

# We use test the source archive in a minimal image; the builder image bundles
# too much developer configuration to simulate a build on a fresh user machine.
docker run \
  --rm \
  --volume="$(cd "$(dirname "$0")" && pwd):/work" \
  --workdir="/work" \
  golang:1.8.1 ./verify-archive.sh
