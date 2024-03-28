#!/usr/bin/env bash

set -euxo pipefail

set +x
COCKROACH_DEV_LICENSE=$(gcloud secrets versions access 1 --secret=cockroach-dev-license)
set -x

bazel build --config crosslinux //pkg/cmd/cockroach-short \
    --bes_keywords integration-test-artifact-build \
    --jobs 100 $(./build/github/engflow-args.sh)

ARTIFACTSDIR=$PWD/artifacts
mkdir -p $ARTIFACTSDIR
COCKROACH=$(bazel info bazel-bin --config=crosslinux)/pkg/cmd/cockroach-short/cockroach-short_/cockroach-short

bazel test //pkg/acceptance:acceptance_test \
  --config crosslinux \
  --jobs 100 $(./build/github/engflow-args.sh) \
  --remote_download_minimal \
  "--sandbox_writable_path=$ARTIFACTSDIR" \
  "--test_tmpdir=$ARTIFACTSDIR" \
  --test_arg=-l="$ARTIFACTSDIR" \
  --test_arg=-b=$COCKROACH \
  --test_env=$COCKROACH_DEV_LICENSE \
  --test_env=TZ=America/New_York \
  --test_timeout=1800 \
  --build_event_binary_file=bes.bin

# Some unit tests test automatic ballast creation. These ballasts can be
# larger than the maximum artifact size. Remove any artifacts with the
# EMERGENCY_BALLAST filename.
find "$ARTIFACTSDIR" -name "EMERGENCY_BALLAST" -delete
