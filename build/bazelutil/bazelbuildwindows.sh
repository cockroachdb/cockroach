#!/usr/bin/env bash

set -xeuo pipefail

# TODO(ricky): switching configurations to build `bazci` for the host before
# building the actual `cockroach` binary introduces some delays. It doesn't
# cause bazel to throw away its entire cache or anything, but it definitely
# rebuilds more than is technically necessary. This merits more investigation.
bazel build //pkg/cmd/bazci --config=ci
$(bazel info bazel-bin)/pkg/cmd/bazci/bazci_/bazci --compilation_mode opt \
		       --config ci \
		       --config crosswindows \
		       build //pkg/cmd/cockroach-short
