#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -eo pipefail

_dir="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Execute the common commands for the benchmark runs.
. "$_dir/pebble_nightly_common.sh"

# Run the YCSB benchmark.
#
# NB: We specify "/usr/bin/true" for the --cockroach and --workload binaries to
# prevent roachtest from complaining (and failing) when it can't find
# them. The pebble roachtests don't actually use either cockroach or
# workload.
#
# NB: The ROACHTEST_NAME and ROACHTEST_SUITE variables can be explicitly set to
# run variants, like the YCSB roachtests that run for 10m per run instead of
# 90m: pebble/ycsb/size=64/duration=10 and pebble/ycsb/size=1024/duration=10
# tests are in the "pebble" suite.
timeout -s INT $((1000*60)) bin/roachtest run \
  --slack-token "${SLACK_TOKEN-}" \
  --cluster-id "${TC_BUILD_ID-$(date +"%Y%m%d%H%M%S")}" \
  --cloud "aws" \
  --cockroach "/usr/bin/true" \
  --workload "/usr/bin/true" \
  --artifacts "$artifacts" \
  --artifacts-literal="${LITERAL_ARTIFACTS_DIR:-}" \
  --parallelism 3 \
  --teamcity \
  --cpu-quota=384 \
  --suite "${ROACHTEST_SUITE:-pebble_nightly_ycsb}" \
  "${ROACHTEST_NAME:-pebble}"

exit_status=$?

# If the PEBBLE_SHA is not set, we're running against the tip of master as a
# part of nightly benchmarks and we should report the benchmark results into the
# s3 bucket. If PEBBLE_SHA is set, we don't pollute the nightly benchmark
# results with our non-master runs.

if [[ -z "${PEBBLE_SHA:-}" ]]; then
  build_mkbench

  prepare_datadir

  # Parse the YCSB data. We first pull down the existing data.js file from S3,
  # which will be merged with the data from this run. We then push the merged
  # file back to S3.
  aws s3 cp s3://pebble-benchmarks/data.js data.js
  ./mkbench ycsb
  aws s3 cp data.js s3://pebble-benchmarks/data.js

  sync_data_dir
fi


exit "$exit_status"
