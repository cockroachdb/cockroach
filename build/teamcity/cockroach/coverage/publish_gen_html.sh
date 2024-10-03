#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


# This script is the second step of the "Publish Coverage" build.
#
# It takes some of the lcov files produced by the previous step and generates
# HTML mini-websites.
#
# The inputs expected by this script:
#  - output/unit_tests.lcov
#  - output/roachtests.lcov
#  - output/unit_tests_and_roachtests.lcov
#
# The outputs of this script are:
#  - output/html/unit_tests
#  - output/html/roachtests
#  - output/html/unit_tests_and_roachtests

set -euo pipefail


echo "Downloading lcov..."
curl -fsSL https://github.com/linux-test-project/lcov/releases/download/v2.0/lcov-2.0.tar.gz | tar xz

PATH="$(pwd)/lcov-2.0/bin:$PATH"

profiles=(
  unit_tests
  roachtests
  unit_tests_and_roachtests
)

for p in "${profiles[@]}"; do
  dir="output/html/$p"
  log="output/logs/genhtml-$p.log"
  mkdir -p "$dir"
  echo "Running genhtml for $p (logs in $log)..."
  genhtml --ignore-errors source,unmapped,unused --synthesize-missing \
    --exclude 'external/**' --num-spaces 2 \
    --substitute 's#^#'$(pwd)'/#' --hierarchical \
    "output/$p.lcov" -o "$dir" > "$log" 2>&1
done
