#!/usr/bin/env bash

set -xeuo pipefail

# Get all the .x archives for everything in bazel-bin.
bazel query 'kind("go_test|go_binary|go_transition_binary|go_transition_test|go_library", //pkg/...)' | \
    xargs bazel build --config=ci --config=test

bazel run //pkg:unused_checker --config=ci --config=test
