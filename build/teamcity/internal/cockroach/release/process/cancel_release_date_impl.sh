#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -xeuo pipefail

to=dev-inf+release-dev@cockroachlabs.com

# override dev defaults with production values
if [[ -z "${DRY_RUN}" ]] ; then
  echo "Dry run"
  to=releases@cockroachlabs.com
fi

# run git fetch in order to get all remote branches
git fetch --tags -q origin
bazel build --config=crosslinux //pkg/cmd/release

$(bazel info --config=crosslinux bazel-bin)/pkg/cmd/release/release_/release \
  cancel-release-date \
  --release-series="$RELEASE_SERIES" \
  --template-dir=pkg/cmd/release/templates \
  --smtp-user=cronjob@cockroachlabs.com \
  --smtp-host=smtp.gmail.com \
  --smtp-port=587 \
  --publish-date="$PUBLISH_DATE" \
  --next-publish-date="$NEXT_PUBLISH_DATE" \
  --to=$to
