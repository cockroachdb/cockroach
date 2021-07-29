#!/usr/bin/env bash
set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"
generate_ssh_key
log_into_gcloud

set -x

export ROACHPROD_USER=teamcity

# TODO(erikgrinaker): We should use a dedicated or more appropriate project
# here, but use andrei-jepsen for now. We don't want to use the regular test
# project here to avoid disturbing test runs due to e.g. quota limits. See:
# https://cockroachlabs.atlassian.net/browse/DEVINF-140
export GCE_PROJECT=${GCE_PROJECT-andrei-jepsen}

mkdir -p artifacts

build/builder/mkrelease.sh amd64-linux-gnu build bin/workload bin/roachtest bin/roachprod \
  > artifacts/build.txt 2>&1 || (cat artifacts/build.txt; false)

build/teamcity-roachtest-invoke.sh \
  --cloud=gce \
  --zones=us-central1-b,us-west1-b,europe-west2-b \
  --debug="${DEBUG-false}" \
  --count="${COUNT-16}" \
  --parallelism="${PARALLELISM-16}" \
  --cpu-quota="${CPUQUOTA-1024}" \
  --cluster-id="${TC_BUILD_ID}" \
  --build-tag="${BUILD_TAG}" \
  --create-args="--lifetime=36h" \
  --cockroach="${PWD}/cockroach-linux-2.6.32-gnu-amd64" \
  --artifacts="${PWD}/artifacts" \
  --disable-issue \
  "${TESTS}"
