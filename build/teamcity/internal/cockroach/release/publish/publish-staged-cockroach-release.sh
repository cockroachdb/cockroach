#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname $(dirname "${0}"))))))"
source "$dir/teamcity-support.sh"  # For log_into_gcloud
source "$dir/release/teamcity-support.sh"

tc_start_block "Variable Setup"
version=$(grep -v "^#" "$dir/../pkg/build/version.txt" | head -n1)
prerelease=false
if [[ $version == *"-"* ]]; then
  # Our pre-release version contains a dash symbol, e.g. v22.2.0-alpha.1
  prerelease=true
fi

if ! echo "${version}" | grep -E -o '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[-.0-9A-Za-z]+)?$'; then
  #                                    ^major           ^minor           ^patch         ^preRelease
  # Matching the version name regex from within the cockroach code except
  # for the `metadata` part at the end because Docker tags don't support
  # `+` in the tag name.
  # https://github.com/cockroachdb/cockroach/blob/4c6864b44b9044874488cfedee3a31e6b23a6790/pkg/util/version/version.go#L75
  echo "Invalid version \"${version}\". Must be of the format \"vMAJOR.MINOR.PATCH(-PRERELEASE)?\"."
  exit 1
fi

PUBLISH_LATEST=
if is_latest "$version"; then
  PUBLISH_LATEST=true
fi

release_branch=$(echo "${version}" | grep -E -o '^v[0-9]+\.[0-9]+')

if [[ -z "${DRY_RUN}" ]] ; then
  gcs_bucket="cockroach-release-artifacts-prod"
  gcs_staged_bucket="cockroach-release-artifacts-staged-prod"
  # export the variable to avoid shell escaping
  export gcs_credentials="$GCS_CREDENTIALS_PROD"
  if [[ $prerelease == false ]] ; then
    dockerhub_repository="docker.io/cockroachdb/cockroach"
  else
    dockerhub_repository="docker.io/cockroachdb/cockroach-unstable"
  fi
  gcr_staged_repository="us-docker.pkg.dev/releases-prod/cockroachdb-staged-releases/cockroach"
  gcr_staged_credentials="$GCS_CREDENTIALS_PROD"
  gcr_repository="us-docker.pkg.dev/cockroach-cloud-images/cockroachdb/cockroach"
  gcr_credentials="$GOOGLE_COCKROACH_CLOUD_IMAGES_COCKROACHDB_CREDENTIALS"
  git_repo_for_tag="cockroachdb/cockroach"
else
  gcs_bucket="cockroach-release-artifacts-dryrun"
  gcs_staged_bucket="cockroach-release-artifacts-staged-dryrun"
  # export the variable to avoid shell escaping
  export gcs_credentials="$GCS_CREDENTIALS_DEV"
  dockerhub_repository="docker.io/cockroachdb/cockroach-misc"
  gcr_staged_repository="us-docker.pkg.dev/releases-dev-356314/cockroachdb-staged-releases/cockroach"
  gcr_staged_credentials="$GCS_CREDENTIALS_DEV"
  gcr_repository="us.gcr.io/cockroach-release/cockroach-test"
  gcr_credentials="$GOOGLE_COCKROACH_RELEASE_CREDENTIALS"
  git_repo_for_tag=""
fi

tc_end_block "Variable Setup"

tc_start_block "Verify binaries SHA"
# Make sure that the linux/amd64 source docker image is built using the same version and SHA. 
# This is a quick check and it assumes that the docker image was built correctly and based on the tarball binaries.
docker_login_gcr "$gcr_staged_repository" "$gcr_staged_credentials"
verify_docker_image "${gcr_staged_repository}:${version}" "linux/amd64" "$BUILD_VCS_NUMBER" "$version" false
tc_end_block "Verify binaries SHA"

tc_start_block "Check remote tag and tag"
if [[ -z "${DRY_RUN}" ]]; then
  github_ssh_key="${GITHUB_COCKROACH_TEAMCITY_PRIVATE_SSH_KEY}"
  configure_git_ssh_key
  if git_wrapped ls-remote --exit-code --tags "ssh://git@github.com/${git_repo_for_tag}.git" "${version}"; then
    echo "Tag ${version} already exists"
    exit 1
  fi
  git tag "${version}"
else
  echo "Skipping for dry-run"
fi
tc_end_block "Check remote tag and tag"


tc_start_block "Setup dockerhub credentials"
configure_docker_creds
docker_login
tc_end_block "Setup dockerhub credentials"

tc_start_block "Copy binaries"
export google_credentials="$gcs_credentials"
log_into_gcloud
for product in cockroach cockroach-sql; do
  for platform in linux-amd64 linux-amd64-fips linux-arm64 darwin-10.9-amd64 darwin-11.0-arm64 windows-6.2-amd64; do
      archive_suffix=tgz
      if [[ $platform == *"windows"* ]]; then 
          archive_suffix=zip
      fi
      archive="$product-$version.$platform.$archive_suffix"
      gsutil cp "gs://$gcs_staged_bucket/$archive" "gs://$gcs_bucket/$archive"
      gsutil cp "gs://$gcs_staged_bucket/$archive.sha256sum" "gs://$gcs_bucket/$archive.sha256sum"
  done
done
tc_end_block "Copy binaries"


tc_start_block "Make and push multiarch docker images"
declare -a gcr_arch_tags
declare -a dockerhub_arch_tags
dockerhub_tag="${dockerhub_repository}:${version}"
gcr_tag="${gcr_repository}:${version}"

for platform_name in amd64 arm64; do
  dockerhub_arch_tag="${dockerhub_repository}:${platform_name}-${version}"
  gcr_arch_tag="${gcr_repository}:${platform_name}-${version}"
  gcr_staged_arch_tag="${gcr_staged_repository}:${platform_name}-${version}"
  # Update the packages before pushing to the final destination.
  tmpdir=$(mktemp -d)
  echo "FROM $gcr_staged_arch_tag" > "$tmpdir/Dockerfile"
  echo "RUN microdnf -y --best --refresh upgrade && microdnf clean all && rm -rf /var/cache/yum" >> "$tmpdir/Dockerfile"
  docker_login_gcr "$gcr_staged_repository" "$gcr_staged_credentials"
  docker build --pull --no-cache --platform "linux/$platform_name" \
    --tag "$dockerhub_arch_tag" --tag "$gcr_arch_tag" "$tmpdir"
  docker_login_gcr "$gcr_repository" "$gcr_credentials"
  docker push "$gcr_arch_tag"
  docker push "$dockerhub_arch_tag"
  gcr_arch_tags+=("$gcr_arch_tag")
  dockerhub_arch_tags+=("$dockerhub_arch_tag")
done

docker_login_gcr "$gcr_repository" "$gcr_credentials"

docker manifest rm "${gcr_tag}" || :
docker manifest create "${gcr_tag}" "${gcr_arch_tags[@]}"
docker manifest push "${gcr_tag}"

docker manifest rm "${dockerhub_tag}" || :
docker manifest create "${dockerhub_tag}" "${dockerhub_arch_tags[@]}"
docker manifest push "${dockerhub_tag}"

docker manifest rm "${gcr_repository}:latest" || :
docker manifest create "${gcr_repository}:latest" "${gcr_arch_tags[@]}"
docker manifest rm "${gcr_repository}:latest-${release_branch}" || :
docker manifest create "${gcr_repository}:latest-${release_branch}" "${gcr_arch_tags[@]}"

docker manifest rm "${dockerhub_repository}:latest" || :
docker manifest create "${dockerhub_repository}:latest" "${dockerhub_arch_tags[@]}"
docker manifest rm "${dockerhub_repository}:latest-${release_branch}" || :
docker manifest create "${dockerhub_repository}:latest-${release_branch}" "${dockerhub_arch_tags[@]}"
tc_end_block "Make and push multiarch docker images"


tc_start_block "Make and push FIPS docker image"
gcr_staged_tag_fips="${gcr_staged_repository}:${version}-fips"
gcr_tag_fips="${gcr_repository}:${version}-fips"
dockerhub_tag_fips="${dockerhub_repository}:${version}-fips"
# Update the packages before pushing to the final destination.
tmpdir=$(mktemp -d)
echo "FROM $gcr_staged_tag_fips" > "$tmpdir/Dockerfile"
echo "RUN microdnf -y --best --refresh upgrade && microdnf clean all && rm -rf /var/cache/yum" >> "$tmpdir/Dockerfile"
docker_login_gcr "$gcr_staged_repository" "$gcr_staged_credentials"
docker build --pull --no-cache --platform "linux/amd64" \
  --tag "$dockerhub_tag_fips" --tag "$gcr_tag_fips" "$tmpdir"
docker_login_gcr "$gcr_repository" "$gcr_credentials"
docker push "$gcr_tag_fips"
docker push "$dockerhub_tag_fips"
tc_end_block "Make and push FIPS docker image"


tc_start_block "Push release tag to GitHub"
if [[ -z "${DRY_RUN}" ]]; then
  configure_git_ssh_key
  git_wrapped push "ssh://git@github.com/${git_repo_for_tag}.git" "$version"
else
  echo "skipping for dry-run"
fi
tc_end_block "Push release tag to GitHub"


tc_start_block "Publish binaries and archive as latest"
# Only push the "latest" for our most recent release branch.
# https://github.com/cockroachdb/cockroach/issues/41067
if [[ -n "${PUBLISH_LATEST}" && $prerelease == false ]]; then
  for product in cockroach cockroach-sql; do
    for platform in linux-amd64 linux-amd64-fips linux-arm64 darwin-10.9-amd64 darwin-11.0-arm64 windows-6.2-amd64; do
        archive_suffix=tgz
        if [[ $platform == *"windows"* ]]; then 
            archive_suffix=zip
        fi
        from="$product-$version.$platform.$archive_suffix"
        to="$product-latest.$platform.$archive_suffix"
        gsutil cp "gs://$gcs_bucket/$from" "gs://$gcs_bucket/$to"
        gsutil cp "gs://$gcs_bucket/$from.sha256sum" "gs://$gcs_bucket/$to.sha256sum"
    done
  done
else
  echo "The latest binaries and archive were _not_ updated."
fi
tc_end_block "Publish binaries and archive as latest"


tc_start_block "Tag docker image as latest-RELEASE_BRANCH"
if [[ $prerelease == false ]]; then
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
if [[ -n "${PUBLISH_LATEST}" || $prerelease == true ]]; then
  docker manifest push "${dockerhub_repository}:latest"
else
  echo "The ${dockerhub_repository}:latest docker image tags were _not_ pushed."
fi
tc_end_block "Tag docker images as latest"


tc_start_block "Verify docker images"
error=0

images=("${dockerhub_tag}" "${gcr_tag}")
if [[ $prerelease == false ]]; then
  images+=("${dockerhub_repository}:latest-${release_branch}")
fi
if [[ -n "${PUBLISH_LATEST}" || $prerelease == true ]]; then
  images+=("${dockerhub_repository}:latest")
fi

for img in "${images[@]}"; do
  for platform_name in amd64 arm64; do
    tc_start_block "Verify $img on $platform_name"
    if ! verify_docker_image "$img" "linux/$platform_name" "$BUILD_VCS_NUMBER" "$version" false; then
      error=1
    fi
    tc_end_block "Verify $img on $platform_name"
  done
done

images=("${dockerhub_tag_fips}" "${gcr_tag_fips}")
for img in "${images[@]}"; do
  tc_start_block "Verify $img"
  if ! verify_docker_image "$img" "linux/amd64" "$BUILD_VCS_NUMBER" "$version" true; then
    error=1
  fi
  tc_end_block "Verify $img"
done

if [ $error = 1 ]; then
  echo "ERROR: Docker image verification failed, see logs above"
  exit 1
fi

tc_end_block "Verify docker images"
