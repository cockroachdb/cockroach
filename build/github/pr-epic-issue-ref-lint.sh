#!/usr/bin/env bash

set -euo pipefail

echo "ref: $GITHUB_REF"
echo "workspace: $GITHUB_WORKSPACE"
which bazel

#bazel run //
