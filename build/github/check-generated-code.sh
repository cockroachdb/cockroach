#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail

# This function dumps the output of the given file to $GITHUB_STEP_SUMMARY
# as a build failure message with appropriate formatting.
cat_output() {
    echo '# Build Failed' > $GITHUB_STEP_SUMMARY
    echo '```' >> $GITHUB_STEP_SUMMARY
    cat $1 | tee -a $GITHUB_STEP_SUMMARY
    cat $1 | tail -n 7 | sed 's/^/::error:: /g'
    echo '```' >> $GITHUB_STEP_SUMMARY
}

# Call this function with one argument, the error message to print if the workspace is dirty.
check_workspace_clean() {
  # The workspace is clean iff `git status --porcelain` produces no output. Any
  # output is either an error message or a listing of an untracked/dirty file.
  if [[ "$(git status --porcelain 2>&1)" != "" ]]; then
    echo '# Build Failed' >> $GITHUB_STEP_SUMMARY
    echo '```' >> $GITHUB_STEP_SUMMARY
    (git status | tee -a $GITHUB_STEP_SUMMARY) || true
    (git diff -a | tee -a $GITHUB_STEP_SUMMARY) || true
    echo "====================================================" | tee -a $GITHUB_STEP_SUMMARY
    echo "Some automatically generated code is not up to date." | tee -a $GITHUB_STEP_SUMMARY
    echo $1 >> $GITHUB_STEP_SUMMARY
    echo "::error:: $1"
    echo '```' >> $GITHUB_STEP_SUMMARY
    exit 1
  fi
}

mkdir artifacts

# Buffer noisy output and only print it on failure.
if ! (./build/bazelutil/check.sh &> artifacts/check-out.log || (cat_output artifacts/check-out.log && false)); then
    # The command will output instructions on how to fix the error.
    exit 1
fi
rm artifacts/check-out.log

ENGFLOW_ARGS="--config crosslinux --jobs 100 $(./build/github/engflow-args.sh) --remote_download_minimal"

EXTRA_BAZEL_ARGS="$ENGFLOW_ARGS" \
    COCKROACH_BAZEL_FORCE_GENERATE=1 \
    build/bazelutil/bazel-generate.sh \
    &> artifacts/generate.log || (cat_output artifacts/generate.log && false)
rm artifacts/generate.log

if grep TODO DEPS.bzl; then
    echo '# Build Failed' >> $GITHUB_STEP_SUMMARY
    echo "Found \`TODO\` comment in \`DEPS.bzl\`. Did you run \`./dev generate bazel --mirror\`?" >> $GITHUB_STEP_SUMMARY
    echo "::error::Found \`TODO\` comment in \`DEPS.bzl\`. Did you run \`./dev generate bazel --mirror\`?"
    exit 1
fi
check_workspace_clean "Run \`./dev generate bazel\` to fix this error."

# Run `bazel run //pkg/gen` and ensure nothing changes. This ensures
# generated documentation and checked-in go code are up to date.
bazel run //pkg/gen $ENGFLOW_ARGS
check_workspace_clean "Run \`./dev generate\` to fix this error."
# Run go mod tidy and ensure nothing changes.
# NB: If files are missing from any packages then `go mod tidy` will
# fail. So we need to make sure that `.pb.go` sources are populated.
# This is part of what //pkg/gen does, in addition to generating Go code and
# docs.
bazel run @go_sdk//:bin/go --ui_event_filters=-DEBUG,-info,-stdout,-stderr --noshow_progress mod tidy
check_workspace_clean "Run \`go mod tidy\` to fix this error."

# NB: If this step fails, then some checksum in the code is probably not
# matching up to the "real" checksum for that artifact.
bazel fetch @distdir//:archives

echo '# Build Succeeded' > $GITHUB_STEP_SUMMARY
