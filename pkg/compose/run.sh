#!/usr/bin/env bash

set -euxo pipefail

"$(dirname "${0}")"/prepare.sh

make test PKG=./pkg/compose TAGS=compose TESTTIMEOUT="${TESTTIMEOUT-30m}" TESTFLAGS="${TESTFLAGS--v}"
