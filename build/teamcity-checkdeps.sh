#!/usr/bin/env bash
set -exuo pipefail

export BUILDER_HIDE_GOPATH_SRC=1
exec ./build/builder.sh build/checkdeps.sh
