#!/usr/bin/env bash
set -euxo pipefail
build/builder.sh make testrace TESTFLAGS='-v --verbosity=1 --vmodule=monitor=2' 2>&1 | go-test-teamcity
