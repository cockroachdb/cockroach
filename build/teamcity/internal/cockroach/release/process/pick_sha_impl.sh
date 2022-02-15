#!/usr/bin/env bash

set -xeuo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname $(dirname "${0}"))))))"
source "$dir/teamcity-support.sh"  # For log_into_gcloud

google_credentials="$METADATA_PUBLISHER_GOOGLE_CREDENTIALS_DEV"
to=dev-inf+release-dev@cockroachlabs.com
qualify_bucket=release-automation-dev
release_bucket=release-automation-dev

# override dev defaults with production values
if [[ -z "${DRY_RUN}" ]] ; then
  echo "Dry run"
  google_credentials="$METADATA_PUBLISHER_GOOGLE_CREDENTIALS_PROD"
  to=releases@cockroachlabs.com
  qualify_bucket=release-automation-prod
  release_bucket=release-automation-prod
fi

log_into_gcloud
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.google-credentials.json"

# run git fetch in order to get all remote branches
git fetch -q origin
bazel build --config=crosslinux //pkg/cmd/release

$(bazel info --config=crosslinux bazel-bin)/pkg/cmd/release/release_/release \
  pick-sha \
  ${DRY_RUN:+--dry-run} \
  --release-series=$RELEASE_SERIES \
  --smtp-user=cronjob@cockroachlabs.com \
  --smtp-host=smtp.gmail.com \
  --smtp-port=587 \
  --to=$to \
  --qualify-bucket=$qualify_bucket \
  --qualify-object-prefix=release-qualification \
  --release-bucket=$release_bucket \
  --release-object-prefix=release-candidates
