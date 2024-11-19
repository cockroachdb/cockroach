#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail

FIPS_ENABLED=1 ./build/teamcity/internal/release/process/roachtest-release-qualification.sh
