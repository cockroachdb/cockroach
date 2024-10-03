#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

set +x
export COCKROACH_DEV_LICENSE=$(gcloud secrets versions access 1 --secret=cockroach-dev-license)
set -x

bazel build --config crosslinux //pkg/cmd/cockroach-short \
    --bes_keywords integration-test-artifact-build \
    --jobs 100 $(./build/github/engflow-args.sh)

COCKROACH=$(bazel info bazel-bin --config=crosslinux)/pkg/cmd/cockroach-short/cockroach-short_/cockroach-short

bazel test //pkg/acceptance:acceptance_test \
  --config crosslinux \
  --jobs 100 $(./build/github/engflow-args.sh) \
  --remote_download_minimal \
  --test_arg=-b=$COCKROACH \
  --test_env=COCKROACH_DEV_LICENSE \
  --test_env=COCKROACH_RUN_ACCEPTANCE=true \
  --test_env=TZ=America/New_York \
  --test_timeout=1800 \
  --build_event_binary_file=bes.bin
