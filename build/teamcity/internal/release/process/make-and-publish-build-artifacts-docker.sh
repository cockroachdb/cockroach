#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/release/teamcity-support.sh"

tc_start_block "Variable Setup"

build_name=$(git describe --tags --dirty --match=v[0-9]* 2> /dev/null || git rev-parse --short HEAD;)

# On no match, `grep -Eo` returns 1. `|| echo""` makes the script not error.
release_build_match="$(is_release_or_master_build "$TC_BUILD_BRANCH")"

if [[ -z "${DRY_RUN}" ]] ; then
  if [[ -z "${release_build_match}" ]] ; then
    gcp_credentials=$GOOGLE_CREDENTIALS_CUSTOMIZED
    gcs_credentials=$GOOGLE_CREDENTIALS_CUSTOMIZED
    gcs_bucket="cockroach-customized-builds-artifacts-prod"
    gcr_repository="us-docker.pkg.dev/cockroach-cloud-images/cockroachdb-customized/cockroach-customized"
  else
    gcp_credentials=$GOOGLE_COCKROACH_CLOUD_IMAGES_COCKROACHDB_CREDENTIALS
    gcs_credentials="$GCS_CREDENTIALS_PROD"
    gcs_bucket="cockroach-builds-artifacts-prod"
    gcr_repository="us-docker.pkg.dev/cockroach-cloud-images/cockroachdb/cockroach"
  fi
else
  build_name="${build_name}.dryrun"
  gcp_credentials="$GOOGLE_COCKROACH_RELEASE_CREDENTIALS"
  gcs_credentials="$GCS_CREDENTIALS_DEV"
  gcs_bucket="cockroach-builds-artifacts-dryrun"
  gcr_repository="us.gcr.io/cockroach-release/cockroach-test"
fi

cat << EOF

  build_name:             $build_name
  gcr_repository:         $gcr_repository
  gcs_bucket:             $gcs_bucket
  release_build_match:    $release_build_match

EOF
tc_end_block "Variable Setup"


tc_start_block "Download and extract tarballs"
# Authenticate to GCS to download the per-arch tarballs published by the
# per-platform jobs.
google_credentials="$gcs_credentials"
log_into_gcloud

tmpdir=$(mktemp -d)
# Register a single EXIT trap up front. The buildx builder is created later; the
# cleanup function removes it only if "$builder" has been set by then.
builder=""
cleanup() {
  if [[ -n "$builder" ]]; then
    docker buildx rm "$builder" 2>/dev/null || true
  fi
  rm -rf "$tmpdir"
  remove_files_on_exit
}
trap cleanup EXIT

# Download and extract per-arch tarballs into a build context laid out as
# ${arch}/ subdirectories, matching what build/deploy/Dockerfile expects.
context="$tmpdir/context"
mkdir -p "$context"
cp build/deploy/Dockerfile "$context/Dockerfile"

for platform in linux-amd64 linux-arm64; do
  arch="${platform#linux-}"
  archive="cockroach-${build_name}.${platform}.tgz"
  gcloud storage cp "gs://$gcs_bucket/$archive" "$tmpdir/$archive"
  staging="$tmpdir/staging-${platform}"
  mkdir -p "$staging"
  tar \
    --directory="$staging" \
    --extract \
    --file="$tmpdir/$archive" \
    --ungzip \
    --ignore-zeros \
    --strip-components=1
  mkdir -p "$context/${arch}"
  cp build/deploy/cockroach.sh "$context/${arch}/"
  cp "$staging/cockroach" "$context/${arch}/"
  cp "$staging"/lib/libgeos.so "$staging"/lib/libgeos_c.so "$context/${arch}/"
  cp LICENSE licenses/THIRD-PARTY-NOTICES.txt "$context/${arch}/"
done

# FIPS is amd64-only and is not produced by every build variant (e.g. customized
# builds). Only prepare its build context when the tarball actually exists, so a
# missing FIPS tarball doesn't abort the job before the multi-arch image is built.
fips_archive="cockroach-${build_name}.linux-amd64-fips.tgz"
build_fips=false
if gcloud storage ls "gs://$gcs_bucket/$fips_archive" >/dev/null 2>&1; then
  build_fips=true
  fips_context="$tmpdir/fips-context"
  mkdir -p "$fips_context/amd64"
  cp build/deploy/Dockerfile "$fips_context/Dockerfile"
  gcloud storage cp "gs://$gcs_bucket/$fips_archive" "$tmpdir/$fips_archive"
  fips_staging="$tmpdir/staging-linux-amd64-fips"
  mkdir -p "$fips_staging"
  tar \
    --directory="$fips_staging" \
    --extract \
    --file="$tmpdir/$fips_archive" \
    --ungzip \
    --ignore-zeros \
    --strip-components=1
  cp build/deploy/cockroach.sh "$fips_context/amd64/"
  cp "$fips_staging/cockroach" "$fips_context/amd64/"
  cp "$fips_staging"/lib/libgeos.so "$fips_staging"/lib/libgeos_c.so "$fips_context/amd64/"
  cp LICENSE licenses/THIRD-PARTY-NOTICES.txt "$fips_context/amd64/"
else
  echo "FIPS tarball $fips_archive not found in gs://$gcs_bucket; skipping FIPS image"
fi
tc_end_block "Download and extract tarballs"


tc_start_block "Build and push multi-arch docker image"
docker_login_gcr "$gcr_repository" "$gcp_credentials"

# Create a buildx builder for multi-platform builds. Setting "$builder" arms the
# EXIT trap (registered above) to remove it.
builder="build-artifacts-builder-$$"
docker buildx rm "$builder" 2>/dev/null || true
docker buildx create --name "$builder" --use

gcr_tag="${gcr_repository}:${build_name}"
docker buildx build --pull --push --no-cache \
  --platform linux/amd64,linux/arm64 \
  --tag "$gcr_tag" "$context"
tc_end_block "Build and push multi-arch docker image"


if [[ "$build_fips" == true ]]; then
  tc_start_block "Build and push FIPS docker image"
  gcr_tag_fips="${gcr_repository}:${build_name}-fips"
  docker buildx build --pull --push --no-cache \
    --platform linux/amd64 \
    --build-arg fips_enabled=1 \
    --tag "$gcr_tag_fips" "$fips_context"
  tc_end_block "Build and push FIPS docker image"
fi


tc_start_block "Verify docker images"
error=0
for arch in amd64 arm64; do
    tc_start_block "Verify $gcr_tag on $arch"
    if ! verify_docker_image "$gcr_tag" "linux/$arch" "$BUILD_VCS_NUMBER" "$build_name" false; then
      error=1
    fi
    tc_end_block "Verify $gcr_tag on $arch"
done

if [[ "$build_fips" == true ]]; then
  tc_start_block "Verify FIPS docker image"
  if ! verify_docker_image "$gcr_tag_fips" "linux/amd64" "$BUILD_VCS_NUMBER" "$build_name" true; then
    error=1
  fi
  tc_end_block "Verify FIPS docker image"
fi

if [ $error = 1 ]; then
  echo "ERROR: Docker image verification failed, see logs above"
  exit 1
fi
tc_end_block "Verify docker images"
