#!/usr/bin/env bash
set -euo pipefail

bin/roachtest run \
  --cloud="${CLOUD}" \
  --artifacts="${ARTIFACTS}" \
  --parallelism="${PARALLELISM}" \
  --cpu-quota="${CPUQUOTA}" \
  --zones="${ZONES}" \
  --count="${COUNT-1}" \
  --debug="${DEBUG-false}" \
  --build-tag="${BUILD_TAG}" \
  --cockroach="${COCKROACH_BINARY}" \
  --roachprod="${PWD}/bin/roachprod" \
  --workload="${PWD}/bin/workload" \
  --teamcity=true \
  --slack-token="${SLACK_TOKEN}" \
  --cluster-id="${TC_BUILD_ID}" \
  "${TESTS}"
