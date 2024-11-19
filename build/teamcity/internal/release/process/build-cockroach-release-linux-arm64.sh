#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


PLATFORM=linux-arm64 ./build/teamcity/internal/release/process/build-cockroach-release-per-platform.sh
