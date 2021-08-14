#!/usr/bin/env bash

set -xeuo pipefail

if [ -z "$1" ]
then
    echo 'Usage: build_impl.sh CONFIG'
    exit 1
fi

CONFIG="$1"

# Only build generated files on Linux x86_64.
GENFILES_TARGETS=
if [ "$CONFIG" == "crosslinux" ]
then
    DOC_TARGETS=$(grep '^//' docs/generated/bazel_targets.txt)
    GO_TARGETS=$(grep -v '^#' build/bazelutil/checked_in_genfiles.txt | cut -d'|' -f1)
    GENFILES_TARGETS="$DOC_TARGETS $GO_TARGETS"
fi

bazel build //pkg/cmd/bazci --config=ci
$(bazel info bazel-bin)/pkg/cmd/bazci/bazci_/bazci --compilation_mode opt \
		       --config "$CONFIG" \
		       build //pkg/cmd/cockroach-short //c-deps:libgeos $GENFILES_TARGETS
