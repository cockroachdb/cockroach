#!/usr/bin/env bash

set -xeuo pipefail

bazel build //pkg/cmd/bazci //pkg/acceptance:acceptance_test //pkg/cmd/cockroach-short:cockroach-short --config=ci
find $(bazel info bazel-bin)/.. -ls
cp \
    $(bazel info bazel-bin)/pkg/acceptance/acceptance_test_/acceptance_test \
    $(bazel info bazel-bin)/pkg/cmd/cockroach-short/cockroach-short_/cockroach-short \
    $(bazel info bazel-bin)/pkg/cmd/bazci/bazci_/bazci \
    /artifacts/
