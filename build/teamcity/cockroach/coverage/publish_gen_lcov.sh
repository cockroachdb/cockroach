#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


# This script is the first step of the "Publish Coverage" build.
#
# It takes coverage data from dependent builds and generates lcov files.
#
# The inputs expected by this script:
#  - input/unit_tests_nonccl.lcov - from Collect Coverage Unit Tests non-CCL
#  - input/unit_tests_ccl.lcov - from Collect Coverage Unit Tests CCL
#  - input/roachtests/**/gocover.zip - from Collect Coverage Roachtest Nightly
#
# The outputs of this script are:
#  - output/unit_tests_nonccl.lcov
#  - output/unit_tests_nccl.lcov
#  - output/unit_tests.lcov - Unit test coverage for both CCL and non-CCL.
#  - output/roachtests/<escaped-test-path>.lcov - Combined coverage from all nodes in the test.
#  - output/roachtests.lcov - Combined coverage across all roachtests.
#  - output/unit_tests_and_roachtests.lcov - Combined coverage across unit tests and roachtests.

set -euxo pipefail

function run_convert() {
  go run github.com/cockroachdb/code-cov-utils/convert@v1.1.0 "$@"
}

tmpdir=$(mktemp -d)
trap "rm -rf $tmpdir" EXIT

# Delete the output directory in case it's left over from a previous build.
rm -rf output
mkdir output
mkdir output/logs

cp input/unit_tests_ccl.lcov output/
cp input/unit_tests_nonccl.lcov output/
# Generate combined unit test coverage.
run_convert -out output/unit_tests.lcov input/unit_tests_ccl.lcov input/unit_tests_nonccl.lcov

# Generate one lcov file for each roachtest.
mkdir output/roachtests
for zipfile in `find input/roachtests -name gocover.zip`; do
  # Generate a name by replacing slashes with _.
  name=${zipfile//\//_}
  # Remove input_ prefix.
  name=${name#input_}
  # Remove _gocover.zip suffix.
  name=${name%_gocover.zip}

  # Check for empty zip file (so we don't error out below).
  if ! unzip -l "$zipfile" > "$tmpdir/unzip.log" 2>&1; then
    if grep -q "zipfile is empty" "$tmpdir/unzip.log"; then
      echo "No coverage data for roachtests/$name"
      continue
    fi
  fi

  echo "Generating roachtests/$name.lcov..."

  rm -rf "$tmpdir"/*
  unzip -q "$zipfile" -d "$tmpdir"
  run_convert -out output/roachtests/$name.lcov $(find "$tmpdir" -type f -name '*.gocov')
done

# Generate combined roachtest coverage.
run_convert -out output/roachtests.lcov $(find output/roachtests -name '*.lcov')

# Generate unit test + roachtest combined coverage.
run_convert -out output/unit_tests_and_roachtests.lcov output/unit_tests.lcov output/roachtests.lcov
