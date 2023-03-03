#!/usr/bin/env bash

set -euo pipefail

_dir="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Execute the common commands for the benchmark runs.
. "$_dir/pebble_nightly_race_common.sh"

# Run the YCSB benchmark.
#
# NB: We specify "true" for the --cockroach and --workload binaries to
# prevent roachtest from complaining (and failing) when it can't find
# them. The pebble roachtests don't actually use either cockroach or
# workload.
exit_status=0
if ! timeout -s INT $((1000*60)) bin/roachtest run \
  --slack-token "${SLACK_TOKEN-}" \
  --cluster-id "${TC_BUILD_ID-$(date +"%Y%m%d%H%M%S")}" \
  --cloud "aws" \
  --cockroach "true" \
  --workload "true" \
  --artifacts "$artifacts" \
  --artifacts-literal="${LITERAL_ARTIFACTS_DIR:-}" \
  --parallelism 3 \
  --teamcity \
  --cpu-quota=384 \
  pebble tag:pebble_nightly_ycsb_race; then
  exit_status=$?
fi

exit "$exit_status"
