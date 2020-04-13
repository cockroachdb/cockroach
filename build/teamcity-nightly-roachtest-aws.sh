#!/usr/bin/env bash
set -euo pipefail

exit_status=0
bin/roachtest run \
  --build-tag "${BUILD_TAG}" \
  --slack-token "${SLACK_TOKEN}" \
  --cluster-id "${TC_BUILD_ID}" \
  --cloud "aws" \
  --cockroach "$PWD/cockroach.linux-2.6.32-gnu-amd64" \
  --roachprod "$PWD/bin/roachprod" \
  --workload "$PWD/bin/workload" \
  --artifacts "$stats_artifacts" \
  --parallelism 3 \
  --teamcity \
  --cpu-quota=384 \
  "kv(0|95)|ycsb|tpcc/(headroom/n4cpu16)|tpccbench/(nodes=3/cpu=16)"
