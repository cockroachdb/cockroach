#!/usr/bin/env bash

set -euo pipefail

# Regression test for #14284, where builds were erroneously marked as dirty when
# build/variables.mk was out of date due to a race condition in Git.
touch -m -t 197001010000 build/variables.mk
build/builder.sh make .buildinfo/tag
if grep -F --quiet -- dirty .buildinfo/tag; then
  echo "error: build tag recorded as dirty: $(<.buildinfo/tag)" >&2
  exit 1
fi

build/builder.sh make archive ARCHIVE=build/cockroach.src.tgz

# We use test the source archive in a minimal image; the builder image bundles
# too much developer configuration to simulate a build on a fresh user machine.
docker run \
  --rm \
  --volume="$(cd "$(dirname "$0")" && pwd):/work" \
  --workdir="/work" \
  golang:stretch ./verify-archive.sh
