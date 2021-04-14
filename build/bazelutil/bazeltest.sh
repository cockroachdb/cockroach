#!/usr/bin/env bash

set -xeuo pipefail

bazel build //pkg/cmd/bazci
$(bazel info bazel-bin)/pkg/cmd/bazci/bazci_/bazci test //pkg:small_tests //pkg:medium_tests //pkg:large_tests //pkg:enormous_tests
