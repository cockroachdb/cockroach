#!/bin/bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euo pipefail

# root is the absolute path to the root directory of the repository.
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
root="$(cd "$script_dir/../../../.." &> /dev/null && pwd)"
# shellcheck source=/dev/null
source "$root/build/teamcity-bazel-support.sh"  # For BAZEL_IMAGE

if [[ -n "$(git -C "$root" status --porcelain)" ]]; then
  echo "refusing to publish from a dirty checkout" >&2
  exit 1
fi

SHA=$(git -C "$root" rev-parse --short=12 HEAD)
gcloud --project cockroach-dev-inf builds submit \
  "$root" \
  --config="$script_dir/cloudbuild.yaml" \
  --substitutions="_BAZEL_IMAGE=$BAZEL_IMAGE,_SHA=$SHA" \
  --timeout=30m
