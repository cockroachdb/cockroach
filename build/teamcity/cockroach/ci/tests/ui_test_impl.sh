#!/usr/bin/env bash

set -xeuo pipefail

bazel build //pkg/cmd/bazci --config=ci
# Any missing dependencies will cause all of //pkg/ui:test to fail during the
# dependency-download phase, leading to cryptic failures.
# Check for unmirrored dependencies before testing the UI to provide a clear
# error message.
$(bazel info bazel-bin --config=ci)/pkg/cmd/bazci/bazci_/bazci -- test --config=ci //pkg/cmd/mirror/npm:list_unmirrored_dependencies
$(bazel info bazel-bin --config=ci)/pkg/cmd/bazci/bazci_/bazci -- test --config=ci //pkg/ui:test
