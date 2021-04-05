#!/usr/bin/env bash

set -xuo pipefail

bazel test //pkg:small_tests //pkg:medium_tests //pkg:large_tests //pkg:enormous_tests
EXIT_CODE=$?
# Stage artifacts.
cp -r $(bazel info bazel-testlogs) /artifacts/bazel-testlogs
find /artifacts/bazel-testlogs -type f -exec chmod 666 {} +
find /artifacts/bazel-testlogs -type d -exec chmod 777 {} +
exit $EXIT_CODE
