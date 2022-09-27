#!/usr/bin/env bash

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

release_branch=$(echo ${build_name} | grep -E -o '^v[0-9]+\.[0-9]+')

if [[ -z "${DRY_RUN}" ]] ; then
  gcs_bucket="cockroach-release-artifacts-staged-prod"
  google_credentials="$GOOGLE_COCKROACH_CLOUD_IMAGES_COCKROACHDB_CREDENTIALS"
  # export the variable to avoid shell escaping
  export gcs_credentials="$GCS_CREDENTIALS_PROD"
  gcr_repository="us-docker.pkg.dev/cockroach-cloud-images/cockroachdb/cockroach"
  # Used for docker login for gcloud
  gcr_hostname="us-docker.pkg.dev"
  git_repo_for_tag="cockroachdb/cockroach"
else
  gcs_bucket="cockroach-release-artifacts-staged-dryrun"
  google_credentials="$GOOGLE_COCKROACH_RELEASE_CREDENTIALS"
  # export the variable to avoid shell escaping
  export gcs_credentials="$GCS_CREDENTIALS_DEV"
  gcr_repository="us.gcr.io/cockroach-release/cockroach-test"
  gcr_hostname="us.gcr.io"
  git_repo_for_tag="cockroachlabs/release-staging"
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


tc_start_block "Check remote tag"
github_ssh_key="${GITHUB_COCKROACH_TEAMCITY_PRIVATE_SSH_KEY}"
configure_git_ssh_key
if git_wrapped ls-remote --exit-code --tags "ssh://git@github.com/${git_repo_for_tag}.git" "${build_name}"; then
  echo "Tag ${build_name} already exists"
  exit 1
fi
tc_end_block "Check remote tag"


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
$BAZEL_BIN/pkg/cmd/publish-provisional-artifacts/publish-provisional-artifacts_/publish-provisional-artifacts -provisional -release --gcs-bucket="$gcs_bucket"
EOF
tc_end_block "Make and publish release artifacts"


tc_start_block "Make and push docker image"
configure_docker_creds
docker_login_with_google

# TODO: update publish-provisional-artifacts with option to leave one or more cockroach binaries in the local filesystem?

curl -f -s -S -o- "https://storage.googleapis.com/$gcs_bucket/cockroach-${build_name}.linux-amd64.tgz" | tar ixfz - --strip-components 1
cp cockroach lib/libgeos.so lib/libgeos_c.so build/deploy
cp -r licenses build/deploy/

docker build \
  --label version=$version \
  --no-cache \
  --tag=${gcr_repository}:${build_name} \
  build/deploy

docker push "${gcr_repository}:${build_name}"
tc_end_block "Make and push docker image"


tc_start_block "Verify docker image"

error=0

img="${gcr_repository}:${build_name}"

docker rmi "$img"
docker pull "$img"
output=$(docker run "$img" version)
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

build_tag_output=$(docker run "$img" version --build-tag)
  if [ "$build_tag_output" != "$build_name" ]; then
  echo "ERROR: Build tag from 'cockroach version --build-tag' mismatch, expected '$build_name', got '$build_tag_output'"
  error=1
fi

if [ $error = 1 ]; then
  echo "ERROR: Docker image verification failed, see logs above"
  exit 1
fi

tc_end_block "Verify docker image"
