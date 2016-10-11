#!/usr/bin/env bash
set -euxo pipefail
build/builder.sh make build
build/builder.sh make install
build/builder.sh go test -v -c -tags acceptance ./pkg/acceptance
cd pkg/acceptance
../../acceptance.test -nodes 3 -l artifacts/acceptance -test.v -test.timeout 10m --verbosity=1 --vmodule=monitor=2 2>&1 | go-test-teamcity
