#!/usr/bin/env bash

# Copyright 2020 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

source "$(dirname "${0}")/teamcity-mark-build.sh"

mark_build "internal-test"
