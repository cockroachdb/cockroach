#!/usr/bin/env bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


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
    EXTRA_TARGETS="$EXTRA_TARGETS //pkg/cmd/roachprod //pkg/cmd/workload //pkg/cmd/dev //pkg/cmd/bazci //pkg/cmd/bazci/process-bep-file //pkg/cmd/bazci/bazel-github-helper"
fi

EXTRA_ARGS=
# GEOS does not compile on windows.
GEOS_TARGET=//c-deps:libgeos

if [ "$CONFIG" == "crosswindows" ]
then
   EXTRA_ARGS=--enable_runfiles
   GEOS_TARGET=
fi

bazel build //pkg/cmd/bazci
BAZEL_BIN=$(bazel info bazel-bin)
"$BAZEL_BIN/pkg/cmd/bazci/bazci_/bazci" -- build -c opt \
		       --config "$CONFIG" $EXTRA_ARGS \
		       //pkg/cmd/cockroach-short //pkg/cmd/cockroach \
		       //pkg/cmd/cockroach-sql $GEOS_TARGET $EXTRA_TARGETS

if [[ $CONFIG == "crosslinuxfips" ]]; then
    for bin in cockroach cockroach-short cockroach-sql; do
        if ! bazel run @go_sdk//:bin/go -- tool nm "artifacts/bazel-bin/pkg/cmd/$bin/${bin}_/$bin" | grep golang-fips; then
            echo "cannot find golang-fips in $bin, exiting"
            exit 1
        fi
    done
fi
if [[ $CONFIG == "crosslinux" ]]; then
    for bin in cockroach cockroach-short cockroach-sql; do
        if bazel run @go_sdk//:bin/go -- tool nm "artifacts/bazel-bin/pkg/cmd/$bin/${bin}_/$bin" | grep golang-fips; then
            echo "found golang-fips in $bin, exiting"
            exit 1
        fi
    done
fi
