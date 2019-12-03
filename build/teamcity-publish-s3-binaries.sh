#!/usr/bin/env bash

# Any arguments to this script are passed through unmodified to
# ./pkg/cmd/publish-artifacts.

set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

build/builder.sh go install ./pkg/cmd/publish-artifacts
build/builder.sh env \
  AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  TC_BUILD_BRANCH="$TC_BUILD_BRANCH" \
  publish-artifacts "$@"
