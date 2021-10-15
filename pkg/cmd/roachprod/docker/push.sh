#!/bin/bash
set -euo pipefail

# root is the absolute path to the root directory of the repository.
root="$(cd ../../../../ &> /dev/null && pwd)"
source "$root/build/teamcity-bazel-support.sh"  # For BAZEL_IMAGE

SHA=$(git rev-parse --short HEAD)
gcloud --project cockroach-dev-inf builds submit \
  --substitutions=_BAZEL_IMAGE=$BAZEL_IMAGE,_SHA=$SHA,_OWNER=cockroachdb,_REPO=cockroach \
  --timeout=30m

# Patch the existing cronjob configuration
kubectl set image cronjob/roachprod-gc-cronjob roachprod-gc-cronjob=gcr.io/cockroach-dev-inf/cockroachlabs/roachprod:$SHA
