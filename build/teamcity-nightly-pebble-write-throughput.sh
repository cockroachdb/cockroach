#!/usr/bin/env bash
#
# This script runs the Pebble Nightly write-throughput benchmarks.
#
# It is run by the Pebble Nightly - AWS TeamCity build
# configuration.

set -eo pipefail

_dir="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Execute the common commands for the benchmark runs and import common
# variables / constants.
. "$_dir/teamcity-nightly-pebble-common.sh"

# Run the write-throughput benchmark.
#
# NB: We specify "true" for the --cockroach and --workload binaries to
# prevent roachtest from complaining (and failing) when it can't find
# them. The pebble roachtests don't actually use either cockroach or
# workload.
exit_status=0
if ! timeout -s INT 12h bin/roachtest run \
  --build-tag "${build_tag}" \
  --slack-token "${SLACK_TOKEN-}" \
  --cluster-id "${TC_BUILD_ID-$(date +"%Y%m%d%H%M%S")}" \
  --cloud "gce" \
  --cockroach "true" \
  --roachprod "$PWD/bin/roachprod" \
  --workload "true" \
  --artifacts "$artifacts" \
  --parallelism 2 \
  --teamcity \
  --cpu-quota=384 \
  pebble tag:pebble_nightly_write; then
  exit_status=$?
fi

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
