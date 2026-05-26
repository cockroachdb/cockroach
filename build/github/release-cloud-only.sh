#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# GitHub Actions wrapper for publishing the cloud-only cockroach candidate.

set -euo pipefail

exec build/teamcity/internal/release/process/build-cockroach-release-cloud-only.sh
