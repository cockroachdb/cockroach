#!/usr/bin/env bash

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

#BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e LITERAL_ARTIFACTS_DIR=$root/artifacts -e BUILD_TAG -e BUILD_VCS_NUMBER -e CLOUD -e GOOGLE_CREDENTIALS -e COCKROACH_DEV_LICENSE -e COUNT -e GITHUB_API_TOKEN -e GITHUB_ORG -e GITHUB_REPO -e GOOGLE_EPHEMERAL_CREDENTIALS -e SLACK_TOKEN -e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH -e TC_BUILD_ID -e TC_SERVER_URL" \
#			       run_bazel build/teamcity/cockroach/nightlies/microbench_checkpoint_impl.sh

echo "build --config nolintonbuild" >> ~/.bazelrc
echo "build --config=crosslinux" >> ~/.bazelrc

./dev doctor
./dev build roachprod

./bin/roachprod create microbench-cluster -n 2 \
  --lifetime "24h" \
  --clouds gce \
  --gce-machine-type "n2d-highmem-2" \
  --gce-zones="europe-west2-c" \
  --os-volume-size=128

./dev roachprod-bench-wrapper ./pkg/util/... --cluster microbench-cluster --bench-args='-iterations 1 -publishdir=gs://gceworker-herko/output' -- -test.short -test.benchtime=1ns
