#!/usr/bin/env bash

set -xeuo pipefail

bazel build //pkg/cmd/bazci --config=ci
$(bazel info bazel-bin)/pkg/cmd/bazci/bazci_/bazci --compilation_mode opt \
		       --config ci \
		       --config crosslinux \
		       build //pkg/cmd/cockroach-short
