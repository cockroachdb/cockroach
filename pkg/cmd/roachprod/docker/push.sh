#!/bin/bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euo pipefail

# root is the absolute path to the root directory of the repository.
root="$(cd ../../../../ &> /dev/null && pwd)"
source "$root/build/teamcity-bazel-support.sh"  # For BAZEL_IMAGE

if [[ -z ${OWNER+x} ]]; then
    OWNER=cockroachdb
fi
if [[ -z ${REPO+x} ]]; then
    REPO=cockroach
fi

SHA=$(git rev-parse --short HEAD)
gcloud --project cockroach-dev-inf builds submit \
  --substitutions=_BAZEL_IMAGE=$BAZEL_IMAGE,_SHA=$SHA,_OWNER=$OWNER,_REPO=$REPO \
  --timeout=30m

# Patch the existing cronjob configuration only for official builds
if [[ "$OWNER" == "cockroachdb" && "$REPO" == "cockroach" ]]; then
  kubectl set image cronjob/roachprod-gc-cronjob roachprod-gc-cronjob=gcr.io/cockroach-dev-inf/cockroachlabs/roachprod:$SHA
fi
