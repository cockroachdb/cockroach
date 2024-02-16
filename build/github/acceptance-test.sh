#!/usr/bin/env bash

set -euxo pipefail

./build/github/run-bazel.sh build --config crosslinux //pkg/cmd/cockroach-short \
    --jobs 100 $(./build/github/engflow-args.sh)

ARTIFACTSDIR=$PWD/artifacts
mkdir -p $ARTIFACTSDIR
COCKROACH=$(bazel info bazel-bin --config=crosslinux)/pkg/cmd/cockroach-short/cockroach-short_/cockroach-short

./build/github/run-bazel.sh test //pkg/acceptance:acceptance_test \
  --config crosslinux \
  --jobs 100 $(./build/github/engflow-args.sh) \
  --remote_download_minimal \
  "--sandbox_writable_path=$ARTIFACTSDIR" \
  "--test_tmpdir=$ARTIFACTSDIR" \
  --bes_keywords acceptance \
  --test_arg=-l="$ARTIFACTSDIR" \
  --test_arg=-b=$COCKROACH \
  --test_env=TZ=America/New_York \
  --test_timeout=1800 \
  --build_event_binary_file=bes.bin

# Some unit tests test automatic ballast creation. These ballasts can be
# larger than the maximum artifact size. Remove any artifacts with the
# EMERGENCY_BALLAST filename.
find "$ARTIFACTSDIR" -name "EMERGENCY_BALLAST" -delete
