#!/usr/bin/env bash

set -xeuo pipefail

bazel build //pkg/cmd/bazci --config=ci
$(bazel info bazel-bin --config=ci)/pkg/cmd/bazci/bazci_/bazci --config=ci \
		                   test //pkg:small_tests //pkg:medium_tests //pkg:large_tests //pkg:enormous_tests -- \
                                   --profile=/artifacts/profile.gz
