#!/usr/bin/env bash

set -eo pipefail

_dir="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"


if [[ "$GOOGLE_EPHEMERAL_CREDENTIALS" ]]; then
  echo "$GOOGLE_EPHEMERAL_CREDENTIALS" > creds.json
  gcloud auth activate-service-account --key-file=creds.json
  export ROACHPROD_USER=teamcity
else
  echo 'warning: GOOGLE_EPHEMERAL_CREDENTIALS not set' >&2
  echo "Assuming that you've run \`gcloud auth login\` from inside the builder." >&2
fi

set -ux

if [[ ! -f ~/.ssh/id_rsa.pub ]]; then
  ssh-keygen -q -N "" -f ~/.ssh/id_rsa
fi

# The artifacts dir should match up with that supplied by TC.
artifacts=$PWD/artifacts
mkdir -p "${artifacts}"
chmod o+rwx "${artifacts}"
mkdir -p "$PWD/bin"
chmod o+rwx "$PWD/bin"

build_tag=$(git describe --abbrev=0 --tags --match=v[0-9]*)
export build_tag

# Build the roachtest binary.
bazel build //pkg/cmd/roachtest --config ci -c opt
BAZEL_BIN=$(bazel info bazel-bin --config ci -c opt)
cp $BAZEL_BIN/pkg/cmd/roachtest/roachtest_/roachtest bin
chmod a+w bin/roachtest

# Pull in the latest version of Pebble from upstream. The benchmarks run
# against the tip of the 'master' branch. We do this by `go get`ting the
# latest version of the module, and then running `mirror` to update `DEPS.bzl`
# accordingly. Note that we build pebble as a race build.
bazel run @go_sdk//:bin/go get github.com/cockroachdb/pebble@latest
NEW_DEPS_BZL_CONTENT=$(bazel run //pkg/cmd/mirror)
echo "$NEW_DEPS_BZL_CONTENT" > DEPS.bzl
bazel build @com_github_cockroachdb_pebble//cmd/pebble --output_groups=race --config ci -c opt
BAZEL_BIN=$(bazel info bazel-bin --config ci -c opt)
cp $BAZEL_BIN/external/com_github_cockroachdb_pebble/cmd/pebble/pebble_/pebble.race ./pebble.linux
chmod a+w ./pebble.linux

# Set the location of the pebble binary. This is referenced by the roachtests,
# which will push this binary out to all workers in order to run the
# benchmarks.
export PEBBLE_BIN=pebble.linux

# Run the YCSB benchmark.
#
# NB: We specify "true" for the --cockroach and --workload binaries to
# prevent roachtest from complaining (and failing) when it can't find
# them. The pebble roachtests don't actually use either cockroach or
# workload.
exit_status=0
if ! timeout -s INT $((1000*60)) bin/roachtest run \
  --build-tag "${build_tag}" \
  --slack-token "${SLACK_TOKEN-}" \
  --cluster-id "${TC_BUILD_ID-$(date +"%Y%m%d%H%M%S")}" \
  --cloud "aws" \
  --cockroach "true" \
  --workload "true" \
  --artifacts "$artifacts" \
  --artifacts-literal="${LITERAL_ARTIFACTS_DIR:-}" \
  --parallelism 3 \
  --teamcity \
  --cpu-quota=384 \
  pebble tag:pebble_nightly_ycsb_race; then
  exit_status=$?
fi

exit "$exit_status"
