#!/usr/bin/env bash

set -xeuo pipefail

if [ -z "$1" ]
then
    echo 'Usage: build_impl.sh CONFIG'
    exit 1
fi

CONFIG="$1"

# Only build docs on Linux.
DOC_TARGETS=
if [ "$CONFIG" == "crosslinux" ]
then
   DOC_TARGETS=$(grep '^//' docs/generated/bazel_targets.txt)
fi

bazel build //pkg/cmd/bazci --config=ci
$(bazel info bazel-bin)/pkg/cmd/bazci/bazci_/bazci --compilation_mode opt \
		       --config "$CONFIG" \
		       build //pkg/cmd/cockroach-short //c-deps:libgeos $DOC_TARGETS
