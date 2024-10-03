#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

EXTRA_PARAMS=""

if [ "$GITHUB_ACTIONS_BRANCH" == "staging" ]; then
  # enable up to 1 retry (2 attempts, worst-case) per test executable to report flakes but only on release branches and staging.
  EXTRA_PARAMS=" --flaky_test_attempts=2"
fi


bazel test //pkg:all_tests //pkg/ui:lint //pkg/ui:test \
    --config crosslinux --jobs 300 --remote_download_minimal \
    --bes_keywords ci-unit-test --config=use_ci_timeouts \
    --build_event_binary_file=bes.bin $(./build/github/engflow-args.sh) \
    $EXTRA_PARAMS
