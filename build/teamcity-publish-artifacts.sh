#!/usr/bin/env bash

# Any arguments to this script are passed through unmodified to
# ./build/teamcity-publish-s3-binaries.

set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

build/teamcity-publish-s3-binaries.sh "$@"

