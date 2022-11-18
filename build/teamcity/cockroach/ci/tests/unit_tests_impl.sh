#!/usr/bin/env bash

set -xeuo pipefail

bazel build //pkg/cmd/bazci --config=ci
$(bazel info bazel-bin --config=ci)/pkg/cmd/bazci/bazci_/bazci -- test --config=cinolint --config=simplestamp -c fastbuild \
		                  $targets_to_test --profile=/artifacts/profile.gz --test_tag_filters="-broken_in_bazel,-integration"
