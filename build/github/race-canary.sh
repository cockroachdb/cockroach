#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# Runs a canary test under the race detector unconditionally to catch
# race conditions that the selective stressrace job might miss. See
# the Slack thread at:
# https://cockroachlabs.slack.com/archives/C023S0V4YEB/p1770903351769809

set -euxo pipefail

bazel test //pkg/server:server_test \
    --config crosslinux --config race \
    --test_filter=TestSpanStatsFanOut \
    --jobs 100 $(./build/github/engflow-args.sh) \
    --remote_download_minimal \
    --test_timeout=300 \
    --runs_per_test=10 \
    --bes_keywords ci-race-canary \
    --build_event_binary_file=bes.bin
