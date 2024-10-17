#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


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

PUBLISH_LATEST=
if is_latest "$build_name"; then
  PUBLISH_LATEST=true
fi

release_branch=$(echo ${build_name} | grep -E -o '^v[0-9]+\.[0-9]+')

if [[ -z "${DRY_RUN}" ]] ; then
  gcs_bucket="cockroach-release-artifacts-prod"
  google_credentials="$GOOGLE_COCKROACH_CLOUD_IMAGES_COCKROACHDB_CREDENTIALS"
  # export the variable to avoid shell escaping
  export gcs_credentials="$GCS_CREDENTIALS_PROD"
  if [[ -z "${PRE_RELEASE}" ]] ; then
    dockerhub_repository="docker.io/cockroachdb/cockroach"
  else
    dockerhub_repository="docker.io/cockroachdb/cockroach-unstable"
  fi
  gcr_repository="us-docker.pkg.dev/cockroach-cloud-images/cockroachdb/cockroach"
  # Used for docker login for gcloud
  gcr_hostname="us-docker.pkg.dev"
  git_repo_for_tag="cockroachdb/cockroach"
else
  gcs_bucket="cockroach-release-artifacts-dryrun"
  google_credentials="$GOOGLE_COCKROACH_RELEASE_CREDENTIALS"
  # export the variable to avoid shell escaping
  export gcs_credentials="$GCS_CREDENTIALS_DEV"
  dockerhub_repository="docker.io/cockroachdb/cockroach-misc"
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
configure_docker_creds
docker_login_with_google
docker_login

declare -a gcr_arch_tags
declare -a dockerhub_arch_tags

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

  dockerhub_arch_tag="${dockerhub_repository}:${platform_name}-${build_name}"
  gcr_arch_tag="${gcr_repository}:${platform_name}-${build_name}"
  dockerhub_arch_tags+=("$dockerhub_arch_tag")
  gcr_arch_tags+=("$gcr_arch_tag")

  # Tag the arch specific images with only one tag per repository. The manifests will reference the tags.
  docker build \
    --label version="$version" \
    --no-cache \
    --pull \
    --platform="linux/${platform_name}" \
    --tag="${dockerhub_arch_tag}" \
    --tag="${gcr_arch_tag}" \
    "build/deploy-${platform_name}"
  docker push "$gcr_arch_tag"
  docker push "$dockerhub_arch_tag"
done

gcr_tag="${gcr_repository}:${build_name}"
dockerhub_tag="${dockerhub_repository}:${build_name}"
docker manifest rm "${gcr_tag}" || :
docker manifest create "${gcr_tag}" "${gcr_arch_tags[@]}"
docker manifest push "${gcr_tag}"
docker manifest rm "${dockerhub_tag}" || :
docker manifest create "${dockerhub_tag}" "${dockerhub_arch_tags[@]}"
docker manifest push "${dockerhub_tag}"

docker manifest rm "${dockerhub_repository}:latest"
docker manifest create "${dockerhub_repository}:latest" "${dockerhub_arch_tags[@]}"
docker manifest rm "${dockerhub_repository}:latest-${release_branch}" || :
docker manifest create "${dockerhub_repository}:latest-${release_branch}" "${dockerhub_arch_tags[@]}"
tc_end_block "Make and push multiarch docker images"


tc_start_block "Make and push FIPS docker image"
platform_name=amd64-fips
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

dockerhub_tag_fips="${dockerhub_repository}:${build_name}-fips"
gcr_tag_fips="${gcr_repository}:${build_name}-fips"

# Tag the arch specific images with only one tag per repository. The manifests will reference the tags.
docker build \
  --label version="$version" \
  --no-cache \
  --pull \
  --platform="linux/amd64" \
  --tag="${dockerhub_tag_fips}" \
  --tag="${gcr_tag_fips}" \
  --build-arg fips_enabled=1 \
  "build/deploy-${platform_name}"
docker push "$gcr_tag_fips"
docker push "$dockerhub_tag_fips"
tc_end_block "Make and push FIPS docker image"


tc_start_block "Push release tag to GitHub"
configure_git_ssh_key
git_wrapped push "ssh://git@github.com/${git_repo_for_tag}.git" "$build_name"
tc_end_block "Push release tag to GitHub"


tc_start_block "Publish binaries and archive as latest-RELEASE_BRANCH"
# example: v20.1-latest
if [[ -z "$PRE_RELEASE" ]]; then
  #TODO: implement me!
  echo "Pushing latest-RELEASE_BRANCH binaries and archive is not implemented."
else
  echo "Pushing latest-RELEASE_BRANCH binaries and archive is not implemented."
fi
tc_end_block "Publish binaries and archive as latest-RELEASE_BRANCH"


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


tc_start_block "Tag docker image as latest-RELEASE_BRANCH"
if [[ -z "$PRE_RELEASE" ]]; then
  docker manifest push "${dockerhub_repository}:latest-${release_branch}"
else
  echo "The ${dockerhub_repository}:latest-${release_branch} docker image tags were _not_ pushed."
fi
tc_end_block "Tag docker images as latest-RELEASE_BRANCH"


tc_start_block "Tag docker images as latest"
# Only push the "latest" tag for our most recent release branch and for the
# latest unstable release
# https://github.com/cockroachdb/cockroach/issues/41067
# https://github.com/cockroachdb/cockroach/issues/48309
if [[ -n "${PUBLISH_LATEST}" || -n "${PRE_RELEASE}" ]]; then
  docker manifest push "${dockerhub_repository}:latest"
else
  echo "The ${dockerhub_repository}:latest docker image tags were _not_ pushed."
fi
tc_end_block "Tag docker images as latest"


tc_start_block "Verify docker images"

images=(
  "${dockerhub_tag}"
  "${gcr_tag}"
)
if [[ -z "$PRE_RELEASE" ]]; then
  images+=("${dockerhub_repository}:latest-${release_branch}")
fi
if [[ -n "${PUBLISH_LATEST}" || -n "${PRE_RELEASE}" ]]; then
  images+=("${dockerhub_repository}:latest")
fi

error=0

for img in "${images[@]}"; do
  for platform_name in amd64 arm64; do
    docker rmi "$img" || true
    docker pull --platform="linux/${platform_name}" "$img"
    output=$(docker run --platform="linux/${platform_name}" "$img" version)
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
  
    build_tag_output=$(docker run --platform="linux/${platform_name}" "$img" version --build-tag)
    if [ "$build_tag_output" != "$build_name" ]; then
      echo "ERROR: Build tag from 'cockroach version --build-tag' mismatch, expected '$build_name', got '$build_tag_output'"
      error=1
    fi
  done
done

images=(
  "${dockerhub_tag_fips}"
  "${gcr_tag_fips}"
)
for img in "${images[@]}"; do
  docker rmi "$img" || true
  docker pull --platform="linux/amd64" "$img"
  output=$(docker run --platform="linux/amd64" "$img" version)
  build_type=$(grep "^Build Type:" <<< "$output" | cut -d: -f2 | sed 's/ //g')
  sha=$(grep "^Build Commit ID:" <<< "$output" | cut -d: -f2 | sed 's/ //g')
  build_tag=$(grep "^Build Tag:" <<< "$output" | cut -d: -f2 | sed 's/ //g')
  go_version=$(grep "^Go Version:" <<< "$output" | cut -d: -f2 | sed 's/ //g')

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
  if [[ "$go_version" != *"fips" ]]; then
    echo "ERROR: Go version '$go_version' does not contain 'fips'"
    error=1
  fi
  build_tag_output=$(docker run --platform="linux/amd64" "$img" version --build-tag)
  if [ "$build_tag_output" != "$build_name" ]; then
    echo "ERROR: Build tag from 'cockroach version --build-tag' mismatch, expected '$build_name', got '$build_tag_output'"
    error=1
  fi
  openssl_version_output=$(docker run --platform="linux/amd64" "$img" shell -c "openssl version -f")
  if [[ $openssl_version_output != *"FIPS_VERSION"* ]]; then
    echo "ERROR: openssl version '$openssl_version_outpu' does not contain 'FIPS_VERSION'"
    error=1
  fi
done

if [ $error = 1 ]; then
  echo "ERROR: Docker image verification failed, see logs above"
  exit 1
fi

tc_end_block "Verify docker images"
