#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

#
# This script contains common configuration used by the Pebble Nightly race runs.

set -euo pipefail

if [[ "$GOOGLE_EPHEMERAL_CREDENTIALS" ]]; then
  echo "$GOOGLE_EPHEMERAL_CREDENTIALS" > creds.json
  gcloud auth activate-service-account --key-file=creds.json
  export ROACHPROD_USER=teamcity
else
  echo 'warning: GOOGLE_EPHEMERAL_CREDENTIALS not set' >&2
  echo "Assuming that you've run \`gcloud auth login\` from inside the builder." >&2
fi

set -ux

if [[ ! -f ~/.ssh/id_rsa.pub ]]; then
  ssh-keygen -q -N "" -f ~/.ssh/id_rsa
fi

# The artifacts dir should match up with that supplied by TC.
artifacts=$PWD/artifacts
mkdir -p "${artifacts}"
chmod o+rwx "${artifacts}"
mkdir -p "$PWD/bin"
chmod o+rwx "$PWD/bin"

# Build the roachtest binary.
bazel build //pkg/cmd/roachtest -c opt
BAZEL_BIN=$(bazel info bazel-bin -c opt)
cp $BAZEL_BIN/pkg/cmd/roachtest/roachtest_/roachtest bin
chmod a+w bin/roachtest

# Pull in the latest version of Pebble from upstream. The benchmarks run
# against the tip of the 'master' branch. We do this by `go get`ting the
# latest version of the module, and then running `mirror` to update `DEPS.bzl`
# accordingly.
bazel run @go_sdk//:bin/go get github.com/cockroachdb/pebble@master
NEW_DEPS_BZL_CONTENT=$(bazel run //pkg/cmd/mirror/go:mirror)
echo "$NEW_DEPS_BZL_CONTENT" > DEPS.bzl
bazel build @com_github_cockroachdb_pebble//cmd/pebble --config race -c opt
BAZEL_BIN=$(bazel info bazel-bin --config race -c opt)
cp $BAZEL_BIN/external/com_github_cockroachdb_pebble/cmd/pebble/pebble_/pebble ./pebble.linux
chmod a+w ./pebble.linux

# Set the location of the pebble binary. This is referenced by the roachtests,
# which will push this binary out to all workers in order to run the
# benchmarks.
export PEBBLE_BIN=pebble.linux
