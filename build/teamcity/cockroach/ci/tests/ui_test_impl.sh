#!/usr/bin/env bash

set -xeuo pipefail

bazel build //pkg/cmd/bazci --config=ci
ARCH="$(uname -m)"
TARGETS_TO_TEST="//pkg/ui/workspaces/db-console:jest //pkg/ui/workspaces/cluster-ui:jest"
# //pkg/ui/workspaces/db-console:jest is broken under ARM64. 
# See https://github.com/cockroachdb/cockroach/issues/97179
if [[ "$ARCH" == "aarch64" || "$ARCH" == "arm64" ]]; then
    TARGETS_TO_TEST="//pkg/ui/workspaces/cluster-ui:jest"
fi
$(bazel info bazel-bin --config=ci)/pkg/cmd/bazci/bazci_/bazci -- test --config=ci $TARGETS_TO_TEST
