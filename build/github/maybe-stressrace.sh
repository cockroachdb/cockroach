#!/usr/bin/env bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euxo pipefail

# Usage: must provide a base SHA and a PR head SHA as arguments.

if [ -z "$1" ]
then
    echo 'Usage: maybe-stressrace.sh BASESHA HEADSHA'
    exit 1
fi

if [ -z "$2" ]
then
    echo 'Usage: maybe-stressrace.sh BASESHA HEADSHA'
    exit 1
fi

./build/github/maybe-stress-impl.sh $1 $2 race
