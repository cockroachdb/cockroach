#!/usr/bin/env bash

set -euxo pipefail

if [[ "$(uname -m)" =~ (arm64|aarch64)$ ]]; then
  export CROSSLINUX_CONFIG="crosslinuxarm"
else
  export CROSSLINUX_CONFIG="crosslinux"
fi

if [[ "$GOOGLE_CREDENTIALS" ]]; then
  echo "$GOOGLE_CREDENTIALS" > creds.json
  gcloud auth activate-service-account --key-file=creds.json
  export ROACHPROD_USER=teamcity
else
  echo 'warning: GOOGLE_EPHEMERAL_CREDENTIALS not set' >&2
  echo "Assuming that you've run \`gcloud auth login\` from inside the builder." >&2
fi

bazel build --config=$CROSSLINUX_CONFIG --config=ci //pkg/cmd/roachprod

BAZEL_BIN=$(bazel info bazel-bin --config=$CROSSLINUX_CONFIG --config=ci)
#$BAZEL_BIN/pkg/cmd/roachprod/roachprod_/roachprod create microbench-cluster -n 2 \
#  --lifetime "24h" \
#  --clouds gce \
#  --gce-machine-type "n2d-highmem-2" \
#  --gce-zones="europe-west2-c" \
#  --os-volume-size=128

./dev roachprod-bench-wrapper
