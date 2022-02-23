#!/usr/bin/env bash

set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

build/builder.sh mkrelease linux-gnu SUFFIX=.linux-2.6.32-gnu-amd64
build/builder.sh mkrelease darwin    SUFFIX=.darwin-10.9-amd64
build/builder.sh mkrelease windows   SUFFIX=.windows-6.2-amd64.exe
cp cockroach.* artifacts
