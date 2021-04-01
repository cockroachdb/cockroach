#!/usr/bin/env bash

set -xeuo pipefail

bazel build //pkg/cmd/cockroach-short
# Stage artifacts.
cp $(bazel info bazel-bin)/pkg/cmd/cockroach-short/cockroach-short_/cockroach-short /artifacts
