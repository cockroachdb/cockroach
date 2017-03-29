#!/usr/bin/env bash
set -euxo pipefail

mkdir -p artifacts

export BUILDER_HIDE_GOPATH_SRC=1
build/builder.sh env \
		 make -C pkg/sql bigtest \
		 TESTFLAGS='-v' \
		 2>&1 \
    | tee artifacts/test.log \
    | go-test-teamcity
