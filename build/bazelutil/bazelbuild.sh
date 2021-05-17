#!/usr/bin/env bash

set -xeuo pipefail

bazel build --symlink_prefix=.bazel/ //pkg/cmd/bazci
$(bazel info bazel-bin)/pkg/cmd/bazci/bazci_/bazci build --symlink_prefix=.bazel/ //pkg/cmd/cockroach-short
