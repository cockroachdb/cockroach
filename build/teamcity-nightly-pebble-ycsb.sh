#!/usr/bin/env bash
#
# This script runs the Pebble Nightly YCSB benchmarks.
#
# It is run by the Pebble Nightly - AWS TeamCity build
# configuration.

set -eo pipefail

_dir="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Execute the common commands for the benchmark runs.
. "$_dir/teamcity-nightly-pebble-common.sh"

# Run the YCSB benchmark.
#
# NB: We specify "true" for the --cockroach and --workload binaries to
# prevent roachtest from complaining (and failing) when it can't find
# them. The pebble roachtests don't actually use either cockroach or
# workload.
exit_status=0
if ! timeout -s INT $((1000*60)) bin/roachtest run \
  --build-tag "${build_tag}" \
  --slack-token "${SLACK_TOKEN-}" \
  --cluster-id "${TC_BUILD_ID-$(date +"%Y%m%d%H%M%S")}" \
  --cloud "aws" \
  --cockroach "true" \
  --roachprod "$PWD/bin/roachprod" \
  --workload "true" \
  --artifacts "$artifacts" \
  --parallelism 3 \
  --teamcity \
  --cpu-quota=384 \
  pebble tag:pebble_nightly_ycsb; then
  exit_status=$?
fi

build_mkbench
prepare_datadir

# Parse the YCSB data. We first pull down the existing data.js file from S3,
# which will be merged with the data from this run. We then push the merged
# file back to S3.
aws s3 cp s3://pebble-benchmarks/data.js data.js
./mkbench ycsb
aws s3 cp data.js s3://pebble-benchmarks/data.js

sync_data_dir

exit "$exit_status"
