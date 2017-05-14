#!/usr/bin/env bash

set -exuo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

build/builder.sh go install ./pkg/cmd/urlcheck
build/builder.sh urlcheck
