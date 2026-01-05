#!/usr/bin/env bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euxo pipefail

# Usage: must provide a base SHA and a PR head SHA as arguments.

if [ -z "$1" ]
then
    echo 'Usage: build.sh BASESHA HEADSHA'
    exit 1
fi

if [ -z "$2" ]
then
    echo 'Usage: build.sh BASESHA HEADSHA'
    exit 1
fi

bazel build --config crosslinux //pkg/cmd/ci-stress \
      --jobs 50 $(./build/github/engflow-args.sh)

BASESHA="$1"
HEADSHA="$2"

set +e
MERGEBASE=$(git merge-base $BASESHA $HEADSHA)
set -e
if [ -z "$MERGEBASE" ]
then
    echo 'Could not calculate merge base. You may have to rebase your in-progress PR.'
    exit 1
fi

# NB: These jobs will run at a lower priority to ensure that the Essential CI
# jobs (required to merge) will be minimally impacted.
$(bazel info bazel-bin --config=crosslinux)/pkg/cmd/ci-stress/ci-stress_/ci-stress \
    $MERGEBASE --config crosslinux --jobs 100 \
    --remote_execution_priority=-1 \
    --remote_download_minimal \
    --bes_keywords ci-stress --config=use_ci_timeouts \
    --build_event_binary_file=bes.bin \
    $(./build/github/engflow-args.sh)
