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
  src_gcs_bucket="cockroach-release-artifacts-staged-prod"
  s3_bucket="binaries.cockroachdb.com"
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
  src_gcs_bucket="cockroach-release-artifacts-staged-dryrun"
  s3_bucket="cockroach-builds-test"
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

# TODO: consider moving this logic to publish-provisional-artifacts. Or even
# better to move the build logic from publish-provisional-artifacts to Bazel
# and use publish-provisional-artifacts for uploads only.
mkdir -p artifacts/upload
cd artifacts/upload
for platform in \
    darwin-10.9-amd64 \
    linux-amd64 \
    windows-6.2-amd64 \
    linux-3.7.10-gnu-aarch64; do

    # TODO: add darwin-11.0-aarch64 after it's signed

    ext=tgz
    if [ $platform = "windows-6.2-amd64" ]; then
        ext=zip
    fi
    for f in cockroach-${build_name}.${platform}.${ext} cockroach-sql-${build_name}.${platform}.${ext}; do
      curl -f -s -S -o "$f" "https://storage.googleapis.com/$src_gcs_bucket/$f"
      curl -f -s -S -o "$f" "https://storage.googleapis.com/$src_gcs_bucket/$f.sha256sum"
      sha256sum -c "$f.sha256sum"
    done
done
cd -
BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e s3_bucket=$s3_bucket -e gcs_credentials -e gcs_bucket=$gcs_bucket -e build_name=$build_name" run_bazel << 'EOF'
bazel build --config ci //pkg/cmd/cloudupload
BAZEL_BIN=$(bazel info bazel-bin --config ci)
export google_credentials="$gcs_credentials"
source "build/teamcity-support.sh"  # For log_into_gcloud
log_into_gcloud
export GOOGLE_APPLICATION_CREDENTIALS="$PWD/.google-credentials.json"
for platform in \
    darwin-10.9-amd64 \
    linux-amd64 \
    windows-6.2-amd64 \
    linux-3.7.10-gnu-aarch64; do

    # TODO: add darwin-11.0-aarch64 after it's signed

    ext=tgz
    if [ $platform = "windows-6.2-amd64" ]; then
        ext=zip
    fi
    for f in cockroach-${build_name}.${platform}.${ext} cockroach-sql-${build_name}.${platform}.${ext}; do
      $BAZEL_BIN/pkg/cmd/cloudupload/cloudupload_/cloudupload "artifacts/upload/$f" "s3://$s3_bucket/$f"
      $BAZEL_BIN/pkg/cmd/cloudupload/cloudupload_/cloudupload "artifacts/upload/$f" "gcs://$gcs_bucket/$f"
    done
done

EOF

tc_start_block "Make and push docker images"
configure_docker_creds
docker_login_with_google
docker_login

docker pull "${gcr_repository}:${build_name}"
docker tag "${gcr_repository}:${build_name}" "${dockerhub_repository}:$build_name"
docker tag "${gcr_repository}:${build_name}" "${dockerhub_repository}:latest"
docker tag "${gcr_repository}:${build_name}" "${dockerhub_repository}:latest-${release_branch}"

docker push "${dockerhub_repository}:${build_name}"
tc_end_block "Make and push docker images"


tc_start_block "Push release tag to GitHub"
git_wrapped push "ssh://git@github.com/${git_repo_for_tag}.git" "$build_name"
tc_end_block "Push release tag to GitHub"



tc_start_block "Publish S3 binaries and archive as latest"
# Only push the "latest" for our most recent release branch.
# https://github.com/cockroachdb/cockroach/issues/41067
if [[ -n "${PUBLISH_LATEST}" && -z "${PRE_RELEASE}" ]]; then
    BAZEL_SUPPORT_EXTRA_DOCKER_ARGS="-e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e TC_BUILDTYPE_ID -e TC_BUILD_BRANCH=$build_name -e s3_bucket=$s3_bucket -e gcs_credentials -e gcs_bucket=$gcs_bucket" run_bazel << 'EOF'
bazel build --config ci //pkg/cmd/publish-provisional-artifacts
BAZEL_BIN=$(bazel info bazel-bin --config ci)
$BAZEL_BIN/pkg/cmd/publish-provisional-artifacts/publish-provisional-artifacts_/publish-provisional-artifacts -bless -release --s3-bucket "$s3_bucket" --gcs-bucket="$gcs_bucket"
EOF

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
if [[ -n "${PUBLISH_LATEST}" || -n "${PRE_RELEASE}" ]]; then
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
if [[ -n "${PUBLISH_LATEST}" || -n "${PRE_RELEASE}" ]]; then
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
