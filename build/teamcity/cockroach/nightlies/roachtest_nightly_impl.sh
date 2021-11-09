#!/usr/bin/env bash

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"

if [[ ! -f ~/.ssh/id_rsa.pub ]]; then
  ssh-keygen -q -C "roachtest-nightly $(date)" -N "" -f ~/.ssh/id_rsa
fi

bazel build --config crosslinux --config ci --config with_ui -c opt \
      //pkg/cmd/cockroach //pkg/cmd/workload //pkg/cmd/roachtest \
      //pkg/cmd/roachprod //c-deps:libgeos
BAZEL_BIN=$(bazel info bazel-bin --config crosslinux --config ci --config with_ui -c opt)
# Move this stuff to bin for simplicity.
mkdir -p bin
chmod o+rwx bin
cp $BAZEL_BIN/pkg/cmd/cockroach/cockroach_/cockroach bin
cp $BAZEL_BIN/pkg/cmd/roachprod/roachprod_/roachprod bin
cp $BAZEL_BIN/pkg/cmd/roachtest/roachtest_/roachtest bin
cp $BAZEL_BIN/pkg/cmd/workload/workload_/workload    bin
chmod a+w bin/cockroach bin/roachprod bin/roachtest bin/workload
# Stage the geos libs in the appropriate spot.
mkdir -p lib.docker_amd64
chmod o+rwx lib.docker_amd64
cp $BAZEL_BIN/c-deps/libgeos/lib/libgeos.so   lib.docker_amd64
cp $BAZEL_BIN/c-deps/libgeos/lib/libgeos_c.so lib.docker_amd64
chmod a+w lib.docker_amd64/libgeos.so lib.docker_amd64/libgeos_c.so

# Set up Google credentials. Note that we need this for all clouds since we upload
# perf artifacts to Google Storage at the end.
echo "$GOOGLE_EPHEMERAL_CREDENTIALS" > creds.json
gcloud auth activate-service-account --key-file=creds.json
export ROACHPROD_USER=teamcity

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
      #     /artifacts/path/to/test/stats.json
      # to
      #     gs://${bucket}/artifacts/${stats_dir}/path/to/test/stats.json
      #
      # `find` below will expand "{}" as ./path/to/test/stats.json. We need
      # to bend over backwards to remove the `./` prefix or gsutil will have
      # a `.` folder in ${stats_dir}, which we don't want.
      (cd /artifacts && \
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
timeout -s INT $((1200*60)) "build/teamcity-roachtest-invoke.sh" \
  --cloud="${CLOUD}" \
  --count="${COUNT-1}" \
  --parallelism="${PARALLELISM}" \
  --cpu-quota="${CPUQUOTA}" \
  --cluster-id="${TC_BUILD_ID}" \
  --build-tag="${BUILD_TAG}" \
  --cockroach="${PWD}/bin/cockroach" \
  --artifacts=/artifacts \
  --artifacts-literal="${LITERAL_ARTIFACTS_DIR:-}" \
  --slack-token="${SLACK_TOKEN}" \
  "${TESTS}"
