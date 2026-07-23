#!/usr/bin/env bash

# Copyright 2020 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

"$(dirname "${0}")"/prepare.sh

make test PKG=./pkg/compose TAGS=compose TESTTIMEOUT="${TESTTIMEOUT-30m}" TESTFLAGS="${TESTFLAGS--v}"
