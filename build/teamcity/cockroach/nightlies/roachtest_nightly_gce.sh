#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

# Entry point for the nightly roachtests. These are run from CI and require
# appropriate secrets for the ${CLOUD} parameter (along with other things).

if [[ ! -f ~/.ssh/id_rsa.pub ]]; then
  ssh-keygen -q -C "roachtest-nightly $(date)" -N "" -f ~/.ssh/id_rsa
fi

# The artifacts dir should match up with that supplied by TC.
artifacts=$PWD/artifacts
mkdir -p "${artifacts}"
chmod o+rwx "${artifacts}"
mkdir bin
chmod o+rwx bin

# Build binaries. Bazci will put them in `artifacts`, so move them to `bin`.
run_bazel build/teamcity/cockroach/nightlies/roachtest_nightly_build.sh
mv artifacts/bazel-bin/pkg/cmd/cockroach/cockroach_/cockroach bin
mv artifacts/bazel-bin/pkg/cmd/roachprod/roachprod_/roachprod bin
mv artifacts/bazel-bin/pkg/cmd/roachtest/roachtest_/roachtest bin
mv artifacts/bazel-bin/pkg/cmd/workload/workload_/workload bin

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
PARALLELISM=16
CPUQUOTA=1024
TESTS=""
case "${CLOUD}" in
  gce)
    ;;
  aws)
    PARALLELISM=3
    CPUQUOTA=384
    if [ -z "${TESTS}" ]; then
      # NB: anchor ycsb to beginning of line to avoid matching `zfs/ycsb/*` which
      # isn't supported on AWS at time of writing.
      TESTS="kv(0|95)|^ycsb|tpcc/(headroom/n4cpu16)|tpccbench/(nodes=3/cpu=16)|scbench/randomload/(nodes=3/ops=2000/conc=1)|backup/(KMS/n3cpu4)"
    fi
    ;;
  *)
    echo "unknown cloud ${CLOUD}"
    exit 1
    ;;
esac

# Teamcity has a 1300 minute timeout that, when reached, kills the process
# without a stack trace (probably SIGKILL).  We'd love to see a stack trace
# though, so after 1200 minutes, kill with SIGINT which will allow roachtest to
# fail tests and cleanup.
run_bazel timeout -s INT $((1200*60)) "build/teamcity-roachtest-invoke.sh" \
  --cloud="${CLOUD}" \
  --count="${COUNT-1}" \
  --parallelism="${PARALLELISM}" \
  --cpu-quota="${CPUQUOTA}" \
  --cluster-id="${TC_BUILD_ID}" \
  --build-tag="${BUILD_TAG}" \
  --cockroach="${PWD}/bin/cockroach" \
  --artifacts="${artifacts}" \
  --slack-token="${SLACK_TOKEN}" \
  "${TESTS}"
