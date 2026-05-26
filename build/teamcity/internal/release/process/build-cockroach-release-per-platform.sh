#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/release/teamcity-support.sh"
source "$dir/teamcity-bazel-support.sh"  # for run_bazel
#
tc_start_block "Variable Setup"
platform="${PLATFORM:?PLATFORM must be specified}"
telemetry_disabled="${TELEMETRY_DISABLED:-false}"
cockroach_archive_prefix="${COCKROACH_ARCHIVE_PREFIX:-cockroach}"
if [[ $telemetry_disabled == true && $cockroach_archive_prefix == "cockroach" ]]; then
  echo "COCKROACH_ARCHIVE_PREFIX must be set to a non-default value when telemetry is disabled"
  exit 1
fi
version=$(grep -v "^#" "$dir/../pkg/build/version.txt" | head -n1)

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
else
  gcs_bucket="cockroach-release-artifacts-staged-dryrun"
fi

# With WIF (GitHub Actions), credentials are handled via mounted credential
# files rather than JSON key env vars. gcs_credentials may be empty.
if [[ -n "${GCS_CREDENTIALS_PROD:-}" || -n "${GCS_CREDENTIALS_DEV:-}" ]]; then
  if [[ -z "${DRY_RUN}" ]] ; then
    export gcs_credentials="$GCS_CREDENTIALS_PROD"
  else
    export gcs_credentials="$GCS_CREDENTIALS_DEV"
  fi
else
  export gcs_credentials=""
fi

tc_end_block "Variable Setup"


tc_start_block "Make and publish release artifacts"
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH=$version -e gcs_credentials -e gcs_bucket=$gcs_bucket -e platform=$platform -e telemetry_disabled=$telemetry_disabled -e cockroach_archive_prefix=$cockroach_archive_prefix" run_bazel << 'EOF'
bazel build //pkg/cmd/publish-artifacts
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

$BAZEL_BIN/pkg/cmd/publish-artifacts/publish-artifacts_/publish-artifacts release --gcs-bucket="$gcs_bucket" --platform=$platform --third-party-notices-file=/tmp/THIRD-PARTY-NOTICES.txt --telemetry-disabled=$telemetry_disabled --cockroach-archive-prefix=$cockroach_archive_prefix
EOF
tc_end_block "Make and publish release artifacts"


