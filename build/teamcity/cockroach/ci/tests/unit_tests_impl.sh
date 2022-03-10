#!/usr/bin/env bash

set -xeuo pipefail

bazel build //pkg/cmd/bazci --config=ci \
  --remote_cache='https://storage.googleapis.com/test-build-cache-cockroachlabs' \
  --cache_test_results=no
$(bazel info bazel-bin --config=ci)/pkg/cmd/bazci/bazci_/bazci --config=ci \
		                   test //pkg:small_tests //pkg:medium_tests //pkg:large_tests //pkg:enormous_tests -- \
                                   --profile=/artifacts/profile.gz \
                                   --remote_cache='https://storage.googleapis.com/test-build-cache-cockroachlabs' \
                                   --cache_test_results=no
