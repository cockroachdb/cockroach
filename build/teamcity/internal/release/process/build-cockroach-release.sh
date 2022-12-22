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

download_prefix="https://storage.googleapis.com/$gcs_bucket"
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


tc_start_block "Make and publish release S3 artifacts"
# Using publish-provisional-artifacts here is funky. We're directly publishing
# the official binaries, not provisional ones. Legacy naming. To clean up...
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH=$build_name -e gcs_credentials -e gcs_bucket=$gcs_bucket" run_bazel << 'EOF'
bazel build --config ci //pkg/cmd/publish-provisional-artifacts
BAZEL_BIN=$(bazel info bazel-bin --config ci)
export google_credentials="$gcs_credentials"
source "build/teamcity-support.sh"  # For log_into_gcloud
log_into_gcloud
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.google-credentials.json"
$BAZEL_BIN/pkg/cmd/publish-provisional-artifacts/publish-provisional-artifacts_/publish-provisional-artifacts -provisional -release --gcs-bucket="$gcs_bucket"
EOF
tc_end_block "Make and publish release S3 artifacts"


tc_start_block "Make and push multiarch docker images"
configure_docker_creds
docker_login_with_google
docker_login

declare -a gcr_amends
declare -a dockerhub_amends

for platform_name in "${platform_names[@]}"; do
  tarball_arch="$(tarball_arch_from_platform_name "$platform_name")"
  docker_arch="$(docker_arch_from_platform_name "$platform_name")"
  linux_platform=linux
  if [[ $tarball_arch == "aarch64" ]]; then
    linux_platform=linux-3.7.10-gnu
  fi
  # TODO: update publish-provisional-artifacts with option to leave one or more cockroach binaries in the local filesystem
  # NB: tar usually stops reading as soon as it sees an empty block but that makes
  # curl unhappy, so passing `--ignore-zeros` will cause it to read to the end.
  cp --recursive "build/deploy" "build/deploy-${docker_arch}"
  curl \
    --fail \
    --silent \
    --show-error \
    --output /dev/stdout \
    --url "${download_prefix}/cockroach-${build_name}.${linux_platform}-${tarball_arch}.tgz" \
    | tar \
    --directory="build/deploy-${docker_arch}" \
    --extract \
    --file=/dev/stdin \
    --ungzip \
    --ignore-zeros \
    --strip-components=1
  cp --recursive licenses "build/deploy-${docker_arch}"
  # Move the libs where Dockerfile expects them to be
  mv build/deploy-${docker_arch}/lib/* build/deploy-${docker_arch}/
  rmdir build/deploy-${docker_arch}/lib

  dockerhub_arch_tag="${dockerhub_repository}:${docker_arch}-${build_name}"
  gcr_arch_tag="${gcr_repository}:${docker_arch}-${build_name}"
  dockerhub_amends+=("--amend" "$dockerhub_arch_tag")
  gcr_amends+=("--amend" "$gcr_arch_tag")

  # Tag the arch specific images with only one tag per repository. The manifests will reference the tags.
  docker build \
    --label version="$version" \
    --no-cache \
    --pull \
    --platform="linux/${docker_arch}" \
    --tag="${dockerhub_arch_tag}" \
    --tag="${gcr_arch_tag}" \
    "build/deploy-${docker_arch}"
  docker push "$gcr_arch_tag"
  docker push "$dockerhub_arch_tag"
done

gcr_tag="${gcr_repository}:${build_name}"
dockerhub_tag="${dockerhub_repository}:${build_name}"
docker manifest create "${gcr_tag}" "${gcr_amends[@]}"
docker manifest push "${gcr_tag}"
docker manifest create "${dockerhub_tag}" "${dockerhub_amends[@]}"
docker manifest push "${dockerhub_tag}"

docker manifest create "${dockerhub_repository}:latest" "${dockerhub_amends[@]}"
docker manifest create "${dockerhub_repository}:latest-${release_branch}" "${dockerhub_amends[@]}"
tc_end_block "Make and push multiarch docker images"


tc_start_block "Push release tag to GitHub"
configure_git_ssh_key
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
    BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH=$build_name -e gcs_credentials -e gcs_bucket=$gcs_bucket" run_bazel << 'EOF'
bazel build --config ci //pkg/cmd/publish-provisional-artifacts
BAZEL_BIN=$(bazel info bazel-bin --config ci)
export google_credentials="$gcs_credentials"
source "build/teamcity-support.sh"  # For log_into_gcloud
log_into_gcloud
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.google-credentials.json"
$BAZEL_BIN/pkg/cmd/publish-provisional-artifacts/publish-provisional-artifacts_/publish-provisional-artifacts -bless -release --gcs-bucket="$gcs_bucket"
EOF

else
  echo "The latest S3 binaries and archive were _not_ updated."
fi
tc_end_block "Publish S3 binaries and archive as latest"


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
  for platform_name in "${platform_names[@]}"; do
    docker_arch="$(docker_arch_from_platform_name "$platform_name")"
    docker rmi "$img" || true
    docker pull --platform="linux/${docker_arch}" "$img"
    output=$(docker run --platform="linux/${docker_arch}" "$img" version)
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
  
    build_tag_output=$(docker run --platform="linux/${docker_arch}" "$img" version --build-tag)
    if [ "$build_tag_output" != "$build_name" ]; then
      echo "ERROR: Build tag from 'cockroach version --build-tag' mismatch, expected '$build_name', got '$build_tag_output'"
      error=1
    fi
  done
done

if [ $error = 1 ]; then
  echo "ERROR: Docker image verification failed, see logs above"
  exit 1
fi

tc_end_block "Verify docker images"
