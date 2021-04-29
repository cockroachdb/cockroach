#!/usr/bin/env bash
set -euxo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

# Entry point for the nightly roachtests. These are run from CI and require
# appropriate secrets for the ${CLOUD} parameter (along with other things,
# apologies, you're going to have to dig around for them below or even better
# yet, look at the job).

# Note that when this script is called, the cockroach binary to be tested
# already exists in the current directory.
COCKROACH_BINARY="${PWD}/cockroach.linux-2.6.32-gnu-amd64"
chmod +x "${COCKROACH_BINARY}"

if [[ ! -f ~/.ssh/id_rsa.pub ]]; then
  ssh-keygen -q -C "roachtest-nightly $(date)" -N "" -f ~/.ssh/id_rsa
fi

# The artifacts dir should match up with that supplied by TC.
artifacts=$PWD/artifacts
mkdir -p "${artifacts}"
chmod o+rwx "${artifacts}"

# Disable global -json flag.
export PATH=$PATH:$(GOFLAGS=; go env GOPATH)/bin

build/builder/mkrelease.sh amd64-linux-gnu bin/workload bin/roachtest bin/roachprod > "${artifacts}/build.txt" 2>&1 || cat "${artifacts}/build.txt"

# Set up Google credentials. Note that we need this for all clouds since we upload
# perf artifacts to Google Storage at the end.
if [[ "$GOOGLE_EPHEMERAL_CREDENTIALS" ]]; then
  echo "$GOOGLE_EPHEMERAL_CREDENTIALS" > creds.json
  gcloud auth activate-service-account --key-file=creds.json
  export ROACHPROD_USER=teamcity
else
  echo 'warning: GOOGLE_EPHEMERAL_CREDENTIALS not set' >&2
  echo "Assuming that you've run \`gcloud auth login\` from inside the builder." >&2
fi

# Early bind the stats dir. Roachtest invocations can take ages, and we want the
# date at the time of the start of the run (which identifies the version of the
# code run best).
stats_dir="$(date +"%Y%m%d")-${TC_BUILD_ID}"

# Set up a function we'll invoke at the end.
function upload_stats {
 if tc_release_branch; then
      bucket="cockroach-nightly-${CLOUD}"
      if [[ "${CLOUD}" == "gce" ]]; then
          # GCE, having been there first, gets an exemption.
          bucket="cockroach-nightly"
      fi

      remote_artifacts_dir="artifacts-${TC_BUILD_BRANCH}"
      if [[ "${TC_BUILD_BRANCH}" == "master" ]]; then
        # The master branch is special, as roachperf hard-codes
        # the location.
        remote_artifacts_dir="artifacts"
      fi

      # The stats.json files need some path translation:
      #     ${artifacts}/path/to/test/stats.json
      # to
      #     gs://${bucket}/artifacts/${stats_dir}/path/to/test/stats.json
      #
      # `find` below will expand "{}" as ./path/to/test/stats.json. We need
      # to bend over backwards to remove the `./` prefix or gsutil will have
      # a `.` folder in ${stats_dir}, which we don't want.
      (cd "${artifacts}" && \
        while IFS= read -r f; do
          if [[ -n "${f}" ]]; then
            gsutil cp "${f}" "gs://${bucket}/${remote_artifacts_dir}/${stats_dir}/${f}"
          fi
        done <<< "$(find . -name stats.json | sed 's/^\.\///')")
  fi
}

# Upload any stats.json we can find, no matter what happens.
trap upload_stats EXIT

# Set up the parameters for the roachtest invocation.

ARTIFACTS="${artifacts}"
PARALLELISM=16
CPUQUOTA=1024
ZONES=""
TESTS=""
case "${CLOUD}" in
  gce)
    # We specify --zones below so that nodes are created in us-central1-b by
    # default. This reserves us-east1-b (the roachprod default zone) for use by
    # manually created clusters.
    ZONES="us-central1-b,us-west1-b,europe-west2-b"
    ;;
  aws)
    PARALLELISM=3
    CPUQUOTA=384
    if [ -z "${TESTS}" ]; then
      TESTS="kv(0|95)|ycsb|tpcc/(headroom/n4cpu16)|tpccbench/(nodes=3/cpu=16)|scbench/randomload/(nodes=3/ops=2000/conc=1)|backup/(KMS/n3cpu4)"
    fi
    ;;
  *)
    echo "unknown cloud ${CLOUD}"
    exit 1
    ;;
esac

export \
CLOUD="${CLOUD}" \
ARTIFACTS="${ARTIFACTS}" \
PARALLELISM="${PARALLELISM}" \
CPUQUOTA="${CPUQUOTA}" \
ZONES="${ZONES}" \
COUNT="${COUNT-1}" \
DEBUG="${DEBUG-false}" \
BUILD_TAG="${BUILD_TAG}" \
COCKROACH_BINARY="${COCKROACH_BINARY}" \
SLACK_TOKEN="${SLACK_TOKEN}" \
TC_BUILD_ID="${TC_BUILD_ID}" \
TESTS="${TESTS}"

# Teamcity has a 1300 minute timeout that, when reached, kills the process
# without a stack trace (probably SIGKILL).  We'd love to see a stack trace
# though, so after 1200 minutes, kill with SIGINT which will allow roachtest to
# fail tests and cleanup.
timeout -s INT $((1200*60)) "build/teamcity-nightly-roachtest-invoke.sh"
