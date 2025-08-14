#!/usr/bin/env bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


PLATFORM=linux-amd64 TELEMETRY_DISABLED=true ./build/teamcity/internal/release/process/build-cockroach-release-per-platform.sh
