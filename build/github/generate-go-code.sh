#!/usr/bin/env bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euo pipefail

ENGFLOW_ARGS="--config crosslinux --jobs 50 $(./build/github/engflow-args.sh) --remote_download_minimal"

bazel run //pkg/gen:code $ENGFLOW_ARGS
