#!/usr/bin/env bash

set -xeuo pipefail

if [ -z "$1" ]
then
    echo 'Usage: build_impl.sh CONFIG'
    exit 1
fi

CONFIG="$1"

EXTRA_TARGETS=

# Extra targets to build on Linux x86_64 only.
if [ "$CONFIG" == "crosslinux" ]
then
    DOC_TARGETS=$(grep '^//' docs/generated/bazel_targets.txt)
    BINARY_TARGETS="@com_github_cockroachdb_go_test_teamcity//:go-test-teamcity //pkg/cmd/dev //pkg/cmd/workload"
    EXTRA_TARGETS="$DOC_TARGETS $BINARY_TARGETS"
fi

# Extra targets to build on Unix only.
if [ "$CONFIG" != "crosswindows" ]
then
    EXTRA_TARGETS="$EXTRA_TARGETS //pkg/cmd/roachprod"
fi

bazel build -c opt \
      --config "$CONFIG" --config ci --config with_ui \
      --bes_results_url=https://app.buildbuddy.io/invocation/ \
      --bes_backend=grpcs://remote.buildbuddy.io \
      --remote_cache=grpcs://remote.buildbuddy.io \
      --remote_download_toplevel \
      --remote_timeout=3600 \
      --remote_header=x-buildbuddy-api-key=$BUILDBUDDY_API_KEY \
      --experimental_remote_cache_compression \
      --build_metadata=ROLE=CI \
      //pkg/cmd/cockroach-short //pkg/cmd/cockroach \
      //pkg/cmd/cockroach-sql \
      //pkg/cmd/cockroach-oss //c-deps:libgeos $EXTRA_TARGETS

if [[ $CONFIG == "crosslinuxfips" ]]; then
    for bin in cockroach cockroach-short cockroach-sql cockroach-oss; do
        if ! bazel run @go_sdk//:bin/go -- tool nm "artifacts/bazel-bin/pkg/cmd/$bin/${bin}_/$bin" | grep golang-fips; then
            echo "cannot find golang-fips in $bin, exiting"
            exit 1
        fi
    done
fi
if [[ $CONFIG == "crosslinux" ]]; then
    for bin in cockroach cockroach-short cockroach-sql cockroach-oss; do
        if bazel run @go_sdk//:bin/go -- tool nm "artifacts/bazel-bin/pkg/cmd/$bin/${bin}_/$bin" | grep golang-fips; then
            echo "found golang-fips in $bin, exiting"
            exit 1
        fi
    done
fi
