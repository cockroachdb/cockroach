#!/usr/bin/env bash
# TODO: trigger this from pick sha
# TODO: send email after done?

set -euxo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/release/teamcity-support.sh"
source "$dir/teamcity-bazel-support.sh"  # for run_bazel

tc_start_block "Variable Setup"

# Matching the version name regex from within the cockroach code except
# for the `metadata` part at the end because Docker tags don't support
# `+` in the tag name.
# https://github.com/cockroachdb/cockroach/blob/4c6864b44b9044874488cfedee3a31e6b23a6790/pkg/util/version/version.go#L75
build_name="$(echo "${NAME}" | grep -E -o '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[-.0-9A-Za-z]+)?$')"
#                                         ^major           ^minor           ^patch         ^preRelease
version=$(echo ${build_name} | sed -e 's/^v//' | cut -d- -f 1)

if [[ -z "$build_name" ]] ; then
    echo "Invalid NAME \"${NAME}\". Must be of the format \"vMAJOR.MINOR.PATCH(-PRERELEASE)?\"."
    exit 1
fi

if [[ -z "${DRY_RUN}" ]] ; then
  gcs_bucket="cockroach-release-artifacts-staged-prod"
  google_credentials="$GOOGLE_COCKROACH_CLOUD_IMAGES_COCKROACHDB_CREDENTIALS"
  # export the variable to avoid shell escaping
  export gcs_credentials="$GCS_CREDENTIALS_PROD"
  gcr_repository="us-docker.pkg.dev/cockroach-cloud-images/cockroachdb/cockroach"
  # Used for docker login for gcloud
  gcr_hostname="us-docker.pkg.dev"
else
  gcs_bucket="cockroach-release-artifacts-staged-dryrun"
  google_credentials="$GOOGLE_COCKROACH_RELEASE_CREDENTIALS"
  # export the variable to avoid shell escaping
  export gcs_credentials="$GCS_CREDENTIALS_DEV"
  gcr_repository="us.gcr.io/cockroach-release/cockroach-test"
  gcr_hostname="us.gcr.io"
  if [[ -z "$(echo ${build_name} | grep -E -o '^v[0-9]+\.[0-9]+\.[0-9]+$')" ]] ; then
    # Using `.` to match how we usually format the pre-release portion of the
    # version string using '.' separators.
    # ex: v20.2.0-rc.2.dryrun
    build_name="${build_name}.dryrun"
  else
    # Using `-` to put dryrun in the pre-release portion of the version string.
    # ex: v20.2.0-dryrun
    build_name="${build_name}-dryrun"
  fi
fi

tc_end_block "Variable Setup"


tc_start_block "Tag the release"
git tag "${build_name}"
tc_end_block "Tag the release"


tc_start_block "Make and publish release artifacts"
# Using publish-provisional-artifacts here is funky. We're directly publishing
# the official binaries, not provisional ones. Legacy naming. To clean up...
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH=$build_name -e gcs_credentials -e gcs_bucket=$gcs_bucket" run_bazel << 'EOF'
bazel build --config ci //pkg/cmd/publish-provisional-artifacts
BAZEL_BIN=$(bazel info bazel-bin --config ci)
export google_credentials="$gcs_credentials"
source "build/teamcity-support.sh"  # For log_into_gcloud
log_into_gcloud
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.google-credentials.json"
$BAZEL_BIN/pkg/cmd/publish-provisional-artifacts/publish-provisional-artifacts_/publish-provisional-artifacts -provisional -release --gcs-bucket="$gcs_bucket" --output-directory=artifacts
EOF
tc_end_block "Make and publish release artifacts"


tc_start_block "Make and push multiarch docker images"
docker_login_with_google

declare -a gcr_amends

for platform_name in amd64 arm64; do
  cp --recursive "build/deploy" "build/deploy-${platform_name}"
  tar \
    --directory="build/deploy-${platform_name}" \
    --extract \
    --file="artifacts/cockroach-${build_name}.linux-${platform_name}.tgz" \
    --ungzip \
    --ignore-zeros \
    --strip-components=1
  cp --recursive licenses "build/deploy-${platform_name}"
  # Move the libs where Dockerfile expects them to be
  mv build/deploy-${platform_name}/lib/* build/deploy-${platform_name}/
  rmdir build/deploy-${platform_name}/lib

  gcr_arch_tag="${gcr_repository}:${platform_name}-${build_name}"
  gcr_amends+=("--amend" "$gcr_arch_tag")

  # Tag the arch specific images with only one tag per repository. The manifests will reference the tags.
  docker build \
    --label version="$version" \
    --no-cache \
    --pull \
    --platform="linux/${platform_name}" \
    --tag="${gcr_arch_tag}" \
    "build/deploy-${platform_name}"
  docker push "$gcr_arch_tag"
done

gcr_tag="${gcr_repository}:${build_name}"
docker manifest create "${gcr_tag}" "${gcr_amends[@]}"
docker manifest push "${gcr_tag}"

tc_end_block "Make and push multiarch docker images"


tc_start_block "Publish binaries and archive as latest"
# Only push the "latest" for our most recent release branch.
# https://github.com/cockroachdb/cockroach/issues/41067
if [[ -n "${PUBLISH_LATEST}" && -z "${PRE_RELEASE}" ]]; then
    BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH=$build_name -e gcs_credentials -e gcs_bucket=$gcs_bucket" run_bazel << 'EOF'
bazel build --config ci //pkg/cmd/publish-provisional-artifacts
BAZEL_BIN=$(bazel info bazel-bin --config ci)
export google_credentials="$gcs_credentials"
source "build/teamcity-support.sh"  # For log_into_gcloud
log_into_gcloud
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.google-credentials.json"
$BAZEL_BIN/pkg/cmd/publish-provisional-artifacts/publish-provisional-artifacts_/publish-provisional-artifacts -bless -release --gcs-bucket="$gcs_bucket"
EOF

else
  echo "The latest binaries and archive were _not_ updated."
fi
tc_end_block "Publish binaries and archive as latest"


tc_start_block "Verify docker images"

error=0

for platform_name in amd64 arm64; do
  docker rmi "$gcr_tag" || true
  docker pull --platform="linux/${platform_name}" "$gcr_tag"
  output=$(docker run --platform="linux/${platform_name}" "$gcr_tag" version)
  build_type=$(grep "^Build Type:" <<< "$output" | cut -d: -f2 | sed 's/ //g')
  sha=$(grep "^Build Commit ID:" <<< "$output" | cut -d: -f2 | sed 's/ //g')
  build_tag=$(grep "^Build Tag:" <<< "$output" | cut -d: -f2 | sed 's/ //g')

  # Build Type should always be "release"
  if [ "$build_type" != "release" ]; then
    echo "ERROR: Release type mismatch, expected 'release', got '$build_type'"
    error=1
  fi
  if [ "$sha" != "$BUILD_VCS_NUMBER" ]; then
    echo "ERROR: SHA mismatch, expected '$BUILD_VCS_NUMBER', got '$sha'"
    error=1
  fi
  if [ "$build_tag" != "$build_name" ]; then
    echo "ERROR: Build tag mismatch, expected '$build_name', got '$build_tag'"
    error=1
  fi

  build_tag_output=$(docker run --platform="linux/${platform_name}" "$gcr_tag" version --build-tag)
  if [ "$build_tag_output" != "$build_name" ]; then
    echo "ERROR: Build tag from 'cockroach version --build-tag' mismatch, expected '$build_name', got '$build_tag_output'"
    error=1
  fi
done

if [ $error = 1 ]; then
  echo "ERROR: Docker image verification failed, see logs above"
  exit 1
fi

tc_end_block "Verify docker images"
