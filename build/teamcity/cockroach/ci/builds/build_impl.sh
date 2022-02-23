#!/usr/bin/env bash

set -xeuo pipefail

if [ -z "$1" ]
then
    echo 'Usage: build_impl.sh CONFIG'
    exit 1
fi

CONFIG="$1"

# Extra targets to build on Linux x86_64 only.
EXTRA_TARGETS=
if [ "$CONFIG" == "crosslinux" ]
then
    DOC_TARGETS=$(grep '^//' docs/generated/bazel_targets.txt)
    GO_TARGETS=$(grep -v '^#' build/bazelutil/checked_in_genfiles.txt | cut -d'|' -f1)
    BINARY_TARGETS="@com_github_cockroachdb_go_test_teamcity//:go-test-teamcity //pkg/cmd/dev //pkg/cmd/workload"
    EXTRA_TARGETS="$DOC_TARGETS $GO_TARGETS $BINARY_TARGETS"
fi

bazel build //pkg/cmd/bazci --config=ci
$(bazel info bazel-bin --config=ci)/pkg/cmd/bazci/bazci_/bazci --compilation_mode opt \
		       --config "$CONFIG" --config ci --config with_ui \
		       build //pkg/cmd/cockroach-short //pkg/cmd/cockroach \
		       //pkg/cmd/cockroach-oss //c-deps:libgeos $EXTRA_TARGETS
