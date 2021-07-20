#!/usr/bin/env bash
set -euo pipefail

set +e
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
code=$?
set -e

if [[ ${code} -eq 10 ]]; then
  # Exit code 10 indicates that some tests failed, but that roachtest
  # as a whole passed. We want to exit zero in this case so that we
  # can let TeamCity report failing tests without also failing the
  # build. That way, build failures can be used to notify about serious
  # problems that prevent tests from being invoked in the first place.
  code=0
fi

exit ${code}
