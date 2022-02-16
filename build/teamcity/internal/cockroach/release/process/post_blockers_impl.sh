#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname $(dirname "${0}"))))))"
source "$dir/teamcity-support.sh"  # For log_into_gcloud

to=dev-inf+release-dev@cockroachlabs.com

# override dev defaults with production values
if [[ -z "${DRY_RUN}" ]] ; then
  echo "Using production values"
  to=releases@cockroachlabs.com
else
  echo "Dry run"
fi

# run git fetch in order to get all remote branches
git fetch -q origin
bazel build --config=crosslinux //pkg/cmd/release

$(bazel info --config=crosslinux bazel-bin)/pkg/cmd/release/release_/release \
  post-blockers \
  ${DRY_RUN:+--dry-run} \
  --release-series=$RELEASE_SERIES \
  --smtp-user=cronjob@cockroachlabs.com \
  --smtp-host=smtp.gmail.com \
  --smtp-port=587 \
  --to=$to \
