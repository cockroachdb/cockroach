#!/usr/bin/env bash

set -euxo pipefail

"$(dirname "${0}")"/prepare.sh

make test PKG=./pkg/acceptance TAGS=acceptance TESTFLAGS="${TESTFLAGS--v -nodes 3} -l $TMPDIR"
