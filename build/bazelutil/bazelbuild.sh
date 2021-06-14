#!/usr/bin/env bash

set -xeuo pipefail

# TODO(ricky): switching configurations to build `bazci` for the host before
# building the actual `cockroach` binary introduces some delays. It doesn't
# cause bazel to throw away its entire cache or anything, but it definitely
# rebuilds more than is technically necessary. This merits more investigation.
# TODO(ricky): I think it might be necessary to teach `bazci` about the
# configurations instead of asking `bazci` to just pass the configuration flags
# down to Bazel unchanged, but for now it seems fine.
bazel build //pkg/cmd/bazci --config=ci
$(bazel info bazel-bin)/pkg/cmd/bazci/bazci_/bazci build //pkg/cmd/cockroach-short -- --config=ci --config=crosslinux
