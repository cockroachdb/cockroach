#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/release/teamcity-support.sh"
source "$dir/teamcity-bazel-support.sh"  # for run_bazel

tc_start_block "Variable Setup"
platform="${PLATFORM:?PLATFORM must be specified}"
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
  gcr_credentials="$GCS_CREDENTIALS_PROD"
  # export the variable to avoid shell escaping
  export gcs_credentials="$GCS_CREDENTIALS_PROD"
  gcr_staged_repository="us-docker.pkg.dev/releases-prod/cockroachdb-staged-releases/cockroach"
else
  gcs_bucket="cockroach-release-artifacts-staged-dryrun"
  gcr_credentials="$GCS_CREDENTIALS_DEV"
  # export the variable to avoid shell escaping
  export gcs_credentials="$GCS_CREDENTIALS_DEV"
  gcr_staged_repository="us-docker.pkg.dev/releases-dev-356314/cockroachdb-staged-releases/cockroach"
fi

tc_end_block "Variable Setup"


tc_start_block "Make and publish release artifacts"
# Using publish-provisional-artifacts here is funky. We're directly publishing
# the official binaries, not provisional ones. Legacy naming. To clean up...
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH=$version -e gcs_credentials -e gcs_bucket=$gcs_bucket -e platform=$platform" run_bazel << 'EOF'
bazel build //pkg/cmd/publish-provisional-artifacts
BAZEL_BIN=$(bazel info bazel-bin)
export google_credentials="$gcs_credentials"
source "build/teamcity-support.sh"  # For log_into_gcloud
log_into_gcloud
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.google-credentials.json"

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


if [[ $platform == "linux-amd64" || $platform == "linux-arm64" || $platform == "linux-amd64-fips" ]]; then
  tc_start_block "Make and push docker image"
  docker_login_gcr "$gcr_staged_repository" "$gcr_credentials"
  arch="amd64"
  if [[ $platform == "linux-arm64" ]]; then
    arch="arm64"
  fi
  cp --recursive "build/deploy" "build/deploy-${platform}"
  tar \
    --directory="build/deploy-${platform}" \
    --extract \
    --file="artifacts/cockroach-${version}.${platform}.tgz" \
    --ungzip \
    --ignore-zeros \
    --strip-components=1
  cp LICENSE licenses/THIRD-PARTY-NOTICES.txt "build/deploy-${platform}"
  # Move the libs where Dockerfile expects them to be
  mv build/deploy-${platform}/lib/* build/deploy-${platform}/
  rmdir build/deploy-${platform}/lib

  build_docker_tag="${gcr_staged_repository}:${arch}-${version}"
  if [[ $platform == "linux-amd64-fips" ]]; then
    build_docker_tag="${gcr_staged_repository}:${version}-fips"
    docker build --label version="$version_label" --no-cache --pull --platform="linux/${arch}" --tag="${build_docker_tag}" --build-arg fips_enabled=1 "build/deploy-${platform}"
  else
    docker build --label version="$version_label" --no-cache --pull --platform="linux/${arch}" --tag="${build_docker_tag}" "build/deploy-${platform}"
  fi
  docker push "$build_docker_tag"
  tc_end_block "Make and push docker image"
fi


# Here we verify the FIPS image only. The multi-arch image will be verified in the job it's created.
if [[ $platform == "linux-amd64-fips" ]]; then
  tc_start_block "Verify FIPS docker image"
  verify_docker_image "$build_docker_tag" "linux/amd64" "$BUILD_VCS_NUMBER" "$version" true
  tc_end_block "Verify FIPS docker image"
fi
