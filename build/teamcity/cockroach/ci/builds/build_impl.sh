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
    GO_TARGETS=$(grep -v '^#' build/bazelutil/checked_in_genfiles.txt | cut -d'|' -f1)
    BINARY_TARGETS="@com_github_cockroachdb_go_test_teamcity//:go-test-teamcity //pkg/cmd/dev //pkg/cmd/workload"
    EXTRA_TARGETS="$DOC_TARGETS $GO_TARGETS $BINARY_TARGETS"
fi

# Extra targets to build on Unix only.
if [ "$CONFIG" != "crosswindows" ]
then
    EXTRA_TARGETS="$EXTRA_TARGETS //pkg/cmd/roachprod"
fi

bazel build //pkg/cmd/bazci --config=ci
$(bazel info bazel-bin --config=ci)/pkg/cmd/bazci/bazci_/bazci -- build -c opt \
		       --config "$CONFIG" --config ci --config with_ui \
		       //pkg/cmd/cockroach-short //pkg/cmd/cockroach \
		       //pkg/cmd/cockroach-sql \
		       //pkg/cmd/cockroach-oss //c-deps:libgeos $EXTRA_TARGETS
