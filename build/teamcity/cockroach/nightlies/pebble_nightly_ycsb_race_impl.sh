#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail

_dir="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Execute the common commands for the benchmark runs.
. "$_dir/pebble_nightly_race_common.sh"

# Run the YCSB benchmark.
#
# NB: We specify "/usr/bin/true" for the --cockroach and --workload binaries to
# prevent roachtest from complaining (and failing) when it can't find
# them. The pebble roachtests don't actually use either cockroach or
# workload.
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
  --suite pebble_nightly_ycsb_race \
  pebble

exit_status=$?

exit "$exit_status"
