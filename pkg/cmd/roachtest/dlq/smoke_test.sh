#!/usr/bin/env bash
#
# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.
#
# Seeds the roachtest GitHub DLQ with a real failed issue-post entry.
#
# This exercises the writer path only: roachtest attempts to post a GitHub
# issue with an invalid token, the post fails, and the DLQ wrapper persists
# the failed request to GCS under failed/.
#
# Replay is intentionally not handled here. Use the Cloud Run Job against the
# dev bucket to verify replay behavior.
#
# Prerequisites:
#   - gcloud auth application-default login
#   - The DLQ dev bucket roachtest-github-dlq-dev exists and you have
#     storage.objectAdmin on it
#   - roachtest is on PATH, or ROACHTEST_BIN points at a roachtest binary
#
# Usage:
#   bash pkg/cmd/roachtest/dlq/smoke_test.sh
#
# Optional env overrides:
#   BUCKET           default: roachtest-github-dlq-dev
#   ROACHTEST_BIN    default: roachtest
#   ROACHTEST_NAME   default: roachtest/manual/fail

set -euo pipefail

BUCKET="${BUCKET:-roachtest-github-dlq-dev}"
ROACHTEST_BIN="${ROACHTEST_BIN:-roachtest}"
ROACHTEST_NAME="${ROACHTEST_NAME:-roachtest/manual/fail}"

echo "==> DLQ seed config:"
echo "    bucket: gs://${BUCKET}"
echo "    binary: ${ROACHTEST_BIN}"
echo "    test:   ${ROACHTEST_NAME}"
echo

echo "==> Running roachtest with an invalid token to force a DLQ write"
echo "    (--local --cockroach-stage=latest avoids GCE provisioning and a"
echo "    full ./dev build cockroach; the staged binary is downloaded from"
echo "    cloud storage)"
echo

GITHUB_DLQ_BUCKET="$BUCKET" \
GITHUB_API_TOKEN="invalid-token-for-smoke-test" \
TC_BUILD_BRANCH="master" \
  "$ROACHTEST_BIN" run "$ROACHTEST_NAME" \
    --local \
    --cockroach-stage=latest || true

echo
echo "==> Verifying entry landed under failed/"
TODAY="$(date -u +%Y%m%d)"
PREFIX="failed/master/${TODAY}/"
if ! gsutil ls "gs://${BUCKET}/${PREFIX}" >/dev/null 2>&1; then
  echo "FAIL: no entries found at gs://${BUCKET}/${PREFIX}" >&2
  exit 1
fi

gsutil ls "gs://${BUCKET}/${PREFIX}"
echo
echo "==> DLQ seed complete."
