#!/bin/bash
set -euxo pipefail

mkdir -p artifacts

build/builder.sh env \
		 make -C pkg/sql bigtest \
		 TESTFLAGS='-v' \
		 2>&1 \
    | tee artifacts/test.log \
    | go-test-teamcity
