#!/usr/bin/env bash
set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

mkdir -p artifacts

build/builder.sh env \
	make testrace \
  PKG=./pkg/sql/logictest
	TESTFLAGS='-v' \
	2>&1 \
	| tee artifacts/testlogicrace.log \
	| go-test-teamcity
