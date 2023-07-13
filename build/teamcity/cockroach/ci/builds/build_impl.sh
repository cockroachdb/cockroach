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
    BINARY_TARGETS="@com_github_cockroachdb_go_test_teamcity//:go-test-teamcity"
    EXTRA_TARGETS="$DOC_TARGETS $BINARY_TARGETS"
fi

# Extra targets to build on Unix only.
if [ "$CONFIG" != "crosswindows" ]
then
    EXTRA_TARGETS="$EXTRA_TARGETS //pkg/cmd/roachprod //pkg/cmd/workload //pkg/cmd/dev"
fi

EXTRA_ARGS=
# GEOS does not compile on windows.
GEOS_TARGET=//c-deps:libgeos

if [ "$CONFIG" == "crosswindows" ]
then
   EXTRA_ARGS=--enable_runfiles
   GEOS_TARGET=
fi

bazel build //pkg/cmd/bazci --config=ci
BAZEL_BIN=$(bazel info bazel-bin --config=ci)
"$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci" -- build -c opt \
		       --config "$CONFIG" --config ci $EXTRA_ARGS \
		       //pkg/cmd/cockroach-short //pkg/cmd/cockroach \
		       //pkg/cmd/cockroach-sql \
		       //pkg/cmd/cockroach-oss $GEOS_TARGET $EXTRA_TARGETS

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
