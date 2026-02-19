#!/usr/bin/env bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


TELEMETRY_DISABLED=true ./build/teamcity/internal/release/process/make-and-publish-build-artifacts-docker.sh
