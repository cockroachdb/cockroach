#!/usr/bin/env bash

set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

build/builder.sh go install ./pkg/cmd/compile-build
build/builder.sh env \
  compile-build
