#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# Docker launcher for the standalone roachtest compile-and-cache step.
# Analogous to roachtest_nightly_gce.sh but only compiles — no test execution.
#
# Usage: roachtest_compile.sh [arch...]
#   where arch is one of: amd64, arm64, amd64-fips, s390x.
# With no arguments, the default set (amd64, arm64, amd64-fips) is built.
# Passing a single arch per TeamCity build lets the per-arch compiles run in
# parallel; artifacts land in the shared GCS build cache (keyed by SHA and
# arch), so downstream roachtest jobs get cache hits regardless of how
# compilation was partitioned.

set -exuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel and configure_bazel_storage_access_token

# Mint the short-lived token Bazel's credential helper uses to fetch private
# dependencies, then forward it into the builder container. Without this the
# helper falls through to its local-dev path (`roachdev`), which does not exist
# on CI agents. Mirrors roachtest_nightly_gce.sh.
#
# The token mechanism (this function, build/bazelutil/credential-helper, and its
# .bazelrc wiring) was introduced in v26.3. This script is backported unchanged
# to older release branches where none of that exists and Bazel fetches
# dependencies by other means, so only mint the token where the function is
# defined.
if declare -F configure_bazel_storage_access_token >/dev/null; then
  configure_bazel_storage_access_token
else
  echo "configure_bazel_storage_access_token not defined on this branch (pre-v26.3);" \
       "skipping token minting and relying on this branch's Bazel dependency auth."
fi

# The standalone compile step only warms the build cache; it never runs tests,
# so it passes --skip-host-tooling-unless-native-arch. roachtest_compile_bits.sh then builds the
# host-architecture tooling (roachtest/roachprod/libgeos) only when the host
# arch is itself one of the requested targets, so exactly one per-arch build
# warms it instead of every build redundantly recompiling it.
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e BUILD_VCS_NUMBER -e GOOGLE_EPHEMERAL_CREDENTIALS -e ROACHPROD_GCE_INFRA_PROJECT -e TC_BUILD_BRANCH -e BAZEL_STORAGE_ACCESS_TOKEN" \
  run_bazel build/teamcity/cockroach/nightlies/roachtest_compile_and_cache.sh --skip-host-tooling-unless-native-arch "$@"
