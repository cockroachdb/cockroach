#!/usr/bin/env bash

set -euxo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tc_start_block "Variable Setup"
export BUILDER_HIDE_GOPATH_SRC=1

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
  bucket="${BUCKET:-binaries.cockroachdb.com}"
  google_credentials="$GOOGLE_COCKROACH_CLOUD_IMAGES_CREDENTIALS"
  if [[ -z "${PRE_RELEASE}" ]] ; then
    dockerhub_repository="docker.io/cockroachdb/cockroach"
  else
    dockerhub_repository="docker.io/cockroachdb/cockroach-unstable"
  fi
  gcr_repository="us.gcr.io/cockroach-cloud-images/cockroach"
  s3_download_hostname="${bucket}"
  git_repo_for_tag="cockroachdb/cockroach"
else
  bucket="${BUCKET:-cockroach-builds-test}"
  google_credentials="$GOOGLE_COCKROACH_RELEASE_CREDENTIALS"
  dockerhub_repository="docker.io/cockroachdb/cockroach-misc"
  gcr_repository="us.gcr.io/cockroach-release/cockroach-test"
  s3_download_hostname="${bucket}.s3.amazonaws.com"
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

# Used for docker login for gcloud
gcr_hostname="us.gcr.io"

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


tc_start_block "Compile publish-provisional-artifacts"
build/builder.sh go install ./pkg/cmd/publish-provisional-artifacts
tc_end_block "Compile publish-provisional-artifacts"


tc_start_block "Make and publish release S3 artifacts"
# Using publish-provisional-artifacts here is funky. We're directly publishing
# the official binaries, not provisional ones. Legacy naming. To clean up...
build/builder.sh env \
  AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  TC_BUILD_BRANCH="$build_name" \
  publish-provisional-artifacts -provisional -release -bucket "$bucket"
tc_end_block "Make and publish release S3 artifacts"


tc_start_block "Make and push docker images"
configure_docker_creds
docker_login_with_google
docker_login

# TODO: update publish-provisional-artifacts with option to leave one or more cockroach binaries in the local filesystem?
curl -f -s -S -o- "https://${s3_download_hostname}/cockroach-${build_name}.linux-amd64.tgz" | tar ixfz - --strip-components 1
cp cockroach lib/libgeos.so lib/libgeos_c.so build/deploy
cp -r licenses build/deploy/

docker build \
  --label version=$version \
  --no-cache \
  --tag=${dockerhub_repository}:{"$build_name",latest,latest-"${release_branch}"} \
  --tag=${gcr_repository}:${build_name} \
  build/deploy

docker push "${dockerhub_repository}:${build_name}"
docker push "${gcr_repository}:${build_name}"
tc_end_block "Make and push docker images"


tc_start_block "Push release tag to GitHub"
git_wrapped push "ssh://git@github.com/${git_repo_for_tag}.git" "$build_name"
tc_end_block "Push release tag to GitHub"


tc_start_block "Publish S3 binaries and archive as latest-RELEASE_BRANCH"
# example: v20.1-latest
if [[ -z "$PRE_RELEASE" ]]; then
  #TODO: implement me!
  echo "Pushing latest-RELEASE_BRANCH S3 binaries and archive is not implemented."
else
  echo "Pushing latest-RELEASE_BRANCH S3 binaries and archive is not implemented."
fi
tc_end_block "Publish S3 binaries and archive as latest-RELEASE_BRANCH"


tc_start_block "Publish S3 binaries and archive as latest"
# Only push the "latest" for our most recent release branch.
# https://github.com/cockroachdb/cockroach/issues/41067
if [[ -n "${PUBLISH_LATEST}" && -z "${PRE_RELEASE}" ]]; then
  build/builder.sh env \
    AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
    AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
    TC_BUILD_BRANCH="$build_name" \
    publish-provisional-artifacts -bless -release -bucket "${bucket}"
else
  echo "The latest S3 binaries and archive were _not_ updated."
fi
tc_end_block "Publish S3 binaries and archive as latest"


tc_start_block "Tag docker image as latest-RELEASE_BRANCH"
if [[ -z "$PRE_RELEASE" ]]; then
  docker push "${dockerhub_repository}:latest-${release_branch}"
else
  echo "The ${dockerhub_repository}:latest-${release_branch} docker image tag was _not_ pushed."
fi
tc_end_block "Tag docker image as latest-RELEASE_BRANCH"


tc_start_block "Tag docker image as latest"
# Only push the "latest" tag for our most recent release branch and for the
# latest unstable release
# https://github.com/cockroachdb/cockroach/issues/41067
# https://github.com/cockroachdb/cockroach/issues/48309
if [[ -n "${PUBLISH_LATEST}" ]]; then
  docker push "${dockerhub_repository}:latest"
else
  echo "The ${dockerhub_repository}:latest docker image tag was _not_ pushed."
fi
tc_end_block "Tag docker image as latest"


tc_start_block "Verify docker images"

images=(
  "${dockerhub_repository}:${build_name}"
  "${gcr_repository}:${build_name}"
)
if [[ -z "$PRE_RELEASE" ]]; then
  images+=("${dockerhub_repository}:latest-${release_branch}")
fi
if [[ -n "${PUBLISH_LATEST}" ]]; then
  images+=("${dockerhub_repository}:latest")
fi

error=0

for img in "${images[@]}"; do
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
done

if [ $error = 1 ]; then
  echo "ERROR: Docker image verification failed, see logs above"
  exit 1
fi

tc_end_block "Verify docker images"
