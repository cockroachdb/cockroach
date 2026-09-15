#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

EXTRA_PARAMS=""

if [[ "$GITHUB_ACTIONS_BRANCH" == "staging" || "$GITHUB_ACTIONS_BRANCH" == trunk-merge/* ]]; then
  # enable up to 1 retry (2 attempts, worst-case) per test executable to report flakes but only on release branches and staging.
  EXTRA_PARAMS=" --flaky_test_attempts=2"
fi


# --nostamp overrides the --stamp that --config=crosslinux turns on. Stamping
# links the volatile BUILD_REV and BUILD_UTCTIME workspace status values into
# every test binary that depends on //pkg/build, which changes those test
# actions' digests on every invocation, so no test result is ever served from
# the remote cache, not even when re-running the same commit. Unstamped test
# binaries are what `dev test` and the TeamCity unit test jobs already use.
bazel test //pkg:all_tests //pkg/ui:lint //pkg/ui:test \
    --config crosslinux --nostamp --jobs 200 --remote_download_minimal \
    --bes_keywords ci-unit-test --config=use_ci_timeouts \
    --build_event_binary_file=bes.bin $(./build/github/engflow-args.sh) \
    $EXTRA_PARAMS
