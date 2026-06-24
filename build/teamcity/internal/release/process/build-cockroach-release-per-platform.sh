#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/release/teamcity-support.sh"
source "$dir/teamcity-bazel-support.sh"  # for run_bazel

tc_start_block "Variable Setup"
platform="${PLATFORM:?PLATFORM must be specified}"
# NOTE: release-23.2 ships the older publish-provisional-artifacts binary,
# which predates the --telemetry-disabled / --cockroach-archive-prefix flags
# added to publish-artifacts post-24.3. release-23.2 does not produce
# telemetry-disabled (IBM) artifacts, so those flags are intentionally absent.
version=$(grep -v "^#" "$dir/../pkg/build/version.txt" | head -n1)
version_label=$(echo "${version}" | sed -e 's/^v//' | cut -d- -f 1)

if ! echo "${version}" | grep -E -o '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[-.0-9A-Za-z]+)?$'; then
  #                                    ^major           ^minor           ^patch         ^preRelease
  # Matching the version name regex from within the cockroach code except
  # for the `metadata` part at the end because Docker tags don't support
  # `+` in the tag name.
  # https://github.com/cockroachdb/cockroach/blob/4c6864b44b9044874488cfedee3a31e6b23a6790/pkg/util/version/version.go#L75
  echo "Invalid version \"${version}\". Must be of the format \"vMAJOR.MINOR.PATCH(-PRERELEASE)?\"."
  exit 1
fi

if [[ -z "${DRY_RUN}" ]] ; then
  gcs_bucket="cockroach-release-artifacts-staged-prod"
  gcr_staged_repository="us-docker.pkg.dev/releases-prod/cockroachdb-staged-releases/cockroach"
else
  gcs_bucket="cockroach-release-artifacts-staged-dryrun"
  gcr_staged_repository="us-docker.pkg.dev/releases-dev-356314/cockroachdb-staged-releases/cockroach"
fi

# With WIF (GitHub Actions), credentials are handled via mounted credential
# files rather than JSON key env vars. gcs_credentials and gcr_credentials may be empty.
if [[ -n "${GCS_CREDENTIALS_PROD:-}" || -n "${GCS_CREDENTIALS_DEV:-}" ]]; then
  if [[ -z "${DRY_RUN}" ]] ; then
    gcr_credentials="$GCS_CREDENTIALS_PROD"
    export gcs_credentials="$GCS_CREDENTIALS_PROD"
  else
    gcr_credentials="$GCS_CREDENTIALS_DEV"
    export gcs_credentials="$GCS_CREDENTIALS_DEV"
  fi
else
  gcr_credentials=""
  export gcs_credentials=""
fi

tc_end_block "Variable Setup"


tc_start_block "Make and publish release artifacts"
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH=$version -e gcs_credentials -e gcs_bucket=$gcs_bucket -e platform=$platform" run_bazel << 'EOF'
# release-23.2 still ships publish-provisional-artifacts; the rename to
# publish-artifacts and the `release` subcommand split happened post-24.3.
bazel build //pkg/cmd/publish-provisional-artifacts
BAZEL_BIN=$(bazel info bazel-bin)
export google_credentials="$gcs_credentials"
source "build/teamcity-support.sh"  # For log_into_gcloud
log_into_gcloud
# Under WIF (GitHub Actions), GOOGLE_APPLICATION_CREDENTIALS is already set to
# the mounted credential file. Only override it for TeamCity (service-account
# key written to .google-credentials.json by log_into_gcloud).
if [[ -z "${CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE:-}" ]]; then
  export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.google-credentials.json"
fi

cat licenses/THIRD-PARTY-NOTICES.txt > /tmp/THIRD-PARTY-NOTICES.txt.tmp
echo "================================================================================" >> /tmp/THIRD-PARTY-NOTICES.txt.tmp 
echo "Additional licenses" >> /tmp/THIRD-PARTY-NOTICES.txt.tmp 
echo >> /tmp/THIRD-PARTY-NOTICES.txt.tmp
echo "================================================================================" >> /tmp/THIRD-PARTY-NOTICES.txt.tmp 
for f in $(cd licenses && ls -1 * | grep -v THIRD-PARTY-NOTICES.txt | sort ); do
  echo >> /tmp/THIRD-PARTY-NOTICES.txt.tmp
  echo >> /tmp/THIRD-PARTY-NOTICES.txt.tmp
  echo >> /tmp/THIRD-PARTY-NOTICES.txt.tmp
  echo "------------------------------------------------------------------------" >> /tmp/THIRD-PARTY-NOTICES.txt.tmp
  echo "Notices from $f" >> /tmp/THIRD-PARTY-NOTICES.txt.tmp
  echo "------------------------------------------------------------------------" >> /tmp/THIRD-PARTY-NOTICES.txt.tmp
  echo >> /tmp/THIRD-PARTY-NOTICES.txt.tmp
  echo >> /tmp/THIRD-PARTY-NOTICES.txt.tmp
  cat "licenses/$f" >> /tmp/THIRD-PARTY-NOTICES.txt.tmp
done

tr -d '\r' < /tmp/THIRD-PARTY-NOTICES.txt.tmp > /tmp/THIRD-PARTY-NOTICES.txt

$BAZEL_BIN/pkg/cmd/publish-provisional-artifacts/publish-provisional-artifacts_/publish-provisional-artifacts -provisional -release --gcs-bucket="$gcs_bucket" --output-directory=artifacts --platform=$platform --third-party-notices-file=/tmp/THIRD-PARTY-NOTICES.txt
EOF
tc_end_block "Make and publish release artifacts"


# Docker images are no longer built here. The per-platform jobs only publish
# tarballs to the staged GCS bucket; the multi-arch docker image (and the FIPS
# image) are built from those tarballs in a single `docker buildx build` step
# by build-cockroach-release-docker.sh, which also verifies them.
