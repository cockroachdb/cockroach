#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_start_block "Check build tag"
# Regression test for #14284, where builds were erroneously marked as dirty when
# build/variables.mk was out of date due to a race condition in Git.
run touch -m -t 197001010000 build/variables.mk
run build/builder.sh make .buildinfo/tag
if grep -F --quiet -- dirty .buildinfo/tag; then
  echo "error: build tag recorded as dirty: $(<.buildinfo/tag)" >&2
  exit 1
fi
tc_end_block "Check build tag"

tc_start_block "Build archive"
# Buffer noisy output and only print it on failure.
run build/builder.sh make archive -Otarget ARCHIVE=build/cockroach.src.tgz &> artifacts/build-archive.log || (cat artifacts/build-archive.log && false)
rm artifacts/build-archive.log
tc_end_block "Build archive"

tc_start_block "Test archive"
# We use test the source archive in a minimal image; the builder image bundles
# too much developer configuration to simulate a build on a fresh user machine.
#
# NB: This docker container runs as root. Be sure to mount any bind volumes as
# read-only to avoid creating root-owned directories and files on the host
# machine.
run docker run \
  --rm \
  --volume="$(cd "$(dirname "$0")" && pwd):/work:ro" \
  --workdir="/work" \
  golang:1.16-buster ./verify-archive.sh
tc_end_block "Test archive"

tc_start_block "Clean up"
# Clean up the archive we produced.
run build/builder.sh rm build/cockroach.src.tgz
tc_end_block "Clean up"
