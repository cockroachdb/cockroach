#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -eo pipefail

_dir="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Execute the common commands for the benchmark runs and import common
# variables / constants.
. "$_dir/pebble_nightly_common.sh"

# Run the write-throughput benchmark.
#
# NB: We specify "/usr/bin/true" for the --cockroach and --workload binaries to
# prevent roachtest from complaining (and failing) when it can't find
# them. The pebble roachtests don't actually use either cockroach or
# workload.
timeout -s INT 12h bin/roachtest run \
  --slack-token "${SLACK_TOKEN-}" \
  --cluster-id "${TC_BUILD_ID-$(date +"%Y%m%d%H%M%S")}" \
  --cloud "gce" \
  --cockroach "/usr/bin/true" \
  --workload "/usr/bin/true" \
  --artifacts "$artifacts" \
  --artifacts-literal="${LITERAL_ARTIFACTS_DIR:-}" \
  --parallelism 2 \
  --teamcity \
  --cpu-quota=384 \
  --suite pebble_nightly_write \
  pebble

exit_status=$?

build_mkbench
prepare_datadir

# Parse the write-throughput data. First we need to pull down the existing
# summary.json file, which is analogous to the data.js file from the YCSB
# benchmarks.
mkdir write-throughput
aws s3 cp s3://pebble-benchmarks/write-throughput/summary.json ./write-throughput/summary.json
./mkbench write
aws s3 sync ./write-throughput s3://pebble-benchmarks/write-throughput

sync_data_dir

exit "$exit_status"
