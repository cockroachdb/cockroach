#!/usr/bin/env bash
set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

source "$(dirname "${0}")/teamcity-support.sh"
maybe_ccache

mkdir -p artifacts

build/builder.sh env \
	make testrace \
	PKG=./pkg/sql/logictest \
	TESTFLAGS='-v' \
	ENABLE_ROCKSDB_ASSERTIONS=1 \
	2>&1 \
	| tee artifacts/testlogicrace.log \
	| go-test-teamcity
