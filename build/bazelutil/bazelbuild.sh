#!/usr/bin/env bash

set -xeuo pipefail

bazel build //pkg/cmd/bazci
$(bazel info bazel-bin)/pkg/cmd/bazci/bazci_/bazci build //pkg/cmd/cockroach-short
