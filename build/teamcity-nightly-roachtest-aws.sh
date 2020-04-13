#!/usr/bin/env bash
set -euo pipefail

# NB: Teamcity has a 1080 minute timeout that, when reached,
# kills the process without a stack trace (probably SIGKILL).
# We'd love to see a stack trace though, so after 1000 minutes,
# kill with SIGINT which will allow roachtest to fail tests and
# cleanup.
#
# NB(2): We specify --zones below so that nodes are created in us-central1-b 
# by default. This reserves us-east1-b (the roachprod default zone) for use
# by manually created clusters.
exit_status=0
if ! timeout -s INT $((1000*60)) bin/roachtest run \
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
  "kv(0|95)|ycsb|tpcc/(headroom/n4cpu16)|tpccbench/(nodes=3/cpu=16)"; then
  exit_status=$?
fi

# Upload any stats.json files to the cockroach-nightly-aws bucket on master.
if [[ "${TC_BUILD_BRANCH}" == "master" ]]; then
    for file in $(find ${artifacts#${PWD}/} -name stats.json); do
        gsutil cp ${file} gs://cockroach-nightly-aws/${file}
    done
fi

exit "$exit_status"
