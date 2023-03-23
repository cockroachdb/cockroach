#!/usr/bin/env bash

set -xeuo pipefail

# Get all the .x archives for everything in bazel-bin.
bazel query 'kind("go_test|go_binary|go_transition_binary|go_transition_test|go_library", //pkg/...)' | \
    xargs bazel build --config=ci --config=test \
          --bes_results_url=https://app.buildbuddy.io/invocation/ \
          --bes_backend=grpcs://remote.buildbuddy.io \
          --remote_cache=grpcs://remote.buildbuddy.io \
          --remote_download_toplevel \
          --remote_timeout=3600 \
          --experimental_remote_cache_compression \
          --build_metadata=ROLE=CI \
          --remote_header=x-buildbuddy-api-key=$BUILDBUDDY_API_KEY

bazel run //pkg:unused_checker --config=ci --config=test \
          --bes_results_url=https://app.buildbuddy.io/invocation/ \
          --bes_backend=grpcs://remote.buildbuddy.io \
          --remote_cache=grpcs://remote.buildbuddy.io \
          --remote_download_toplevel \
          --remote_timeout=3600 \
          --experimental_remote_cache_compression \
          --build_metadata=ROLE=CI \
          --remote_header=x-buildbuddy-api-key=$BUILDBUDDY_API_KEY
