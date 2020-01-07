#!/usr/bin/env bash

set -euxo pipefail

"$(dirname "${0}")"/prepare.sh

make test PKG=./pkg/compose TESTTIMEOUT="${TESTTIMEOUT-30m}" TESTFLAGS="${TESTFLAGS--v}"
