#!/usr/bin/env bash
set -euo pipefail

# NB: We specify --zones below so that nodes are created in us-central1-b
# by default. This reserves us-east1-b (the roachprod default zone) for use
# by manually created clusters.
bin/roachtest run \
  --count="${COUNT-1}" \
  --debug="${DEBUG-false}" \
  --build-tag="${BUILD_TAG}" \
  --slack-token="${SLACK_TOKEN}" \
  --cluster-id="${TC_BUILD_ID}" \
  --zones="us-central1-b,us-west1-b,europe-west2-b" \
  --cockroach="$PWD/cockroach.linux-2.6.32-gnu-amd64" \
  --roachprod="$PWD/bin/roachprod" \
  --workload="$PWD/bin/workload" \
  --artifacts="$stats_artifacts" \
  --parallelism=16 \
  --cpu-quota=1024 \
  --teamcity=true \
  "${TESTS}"


exit "$exit_status"
