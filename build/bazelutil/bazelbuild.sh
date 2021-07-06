#!/usr/bin/env bash

set -xeuo pipefail

if [ -z "$1" ]
then
    echo 'Usage: bazelbuild.sh CONFIG'
    exit 1
fi

bazel build //pkg/cmd/bazci --config=ci
$(bazel info bazel-bin)/pkg/cmd/bazci/bazci_/bazci --compilation_mode opt \
		       --config "$1" \
		       build //pkg/cmd/cockroach-short
