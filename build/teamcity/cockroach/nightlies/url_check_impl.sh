#!/usr/bin/env bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -xeuo pipefail

echo "$GOOGLE_EPHEMERAL_CREDENTIALS" > creds.json
gcloud auth activate-service-account --key-file=creds.json

bazel run //pkg/cmd/urlcheck --run_under="cd $PWD && "
