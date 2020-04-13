#!/usr/bin/env bash
set -euo pipefail

# Note that when this script is called, the cockroach binary to be tested
# already exists in the current directory.

if [[ "$GOOGLE_EPHEMERAL_CREDENTIALS" ]]; then
  echo "$GOOGLE_EPHEMERAL_CREDENTIALS" > creds.json
  gcloud auth activate-service-account --key-file=creds.json
  export ROACHPROD_USER=teamcity
else
  echo 'warning: GOOGLE_EPHEMERAL_CREDENTIALS not set' >&2
  echo "Assuming that you've run \`gcloud auth login\` from inside the builder." >&2
fi

# NB: Teamcity has a 1300 minute timeout that, when reached,
# kills the process without a stack trace (probably SIGKILL).
# We'd love to see a stack trace though, so after 1200 minutes,
# kill with SIGINT which will allow roachtest to fail tests and
# cleanup.
#
# NB(2): We specify --zones below so that nodes are created in us-central1-b
# by default. This reserves us-east1-b (the roachprod default zone) for use
# by manually created clusters.
exit_status=0
if ! timeout -s INT $((1200*60)) bin/roachtest run \
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
  "${TESTS}"; then
  exit_status=$?
fi

# Upload any stats.json files to the cockroach-nightly bucket.
if [[ "${TC_BUILD_BRANCH}" == "master" ]]; then
    for file in $(find ${artifacts#${PWD}/} -name stats.json); do
        gsutil cp ${file} gs://cockroach-nightly/${file}
    done
fi

exit "$exit_status"
