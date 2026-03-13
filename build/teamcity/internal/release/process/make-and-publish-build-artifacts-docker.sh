#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/release/teamcity-support.sh"
#
tc_start_block "Variable Setup"

build_name=$(git describe --tags --dirty --match=v[0-9]* 2> /dev/null || git rev-parse --short HEAD;)
telemetry_disabled="${TELEMETRY_DISABLED:-false}"
cockroach_archive_prefix="${COCKROACH_ARCHIVE_PREFIX:-cockroach}"
if [[ $telemetry_disabled == true && $cockroach_archive_prefix == "cockroach" ]]; then
  echo "COCKROACH_ARCHIVE_PREFIX must be set to a non-default value when telemetry is disabled"
  exit 1
fi

release_build_match="$(is_release_or_master_build "$TC_BUILD_BRANCH")"

if [[ -z "${DRY_RUN}" ]] ; then
  if [[ -z "${release_build_match}" ]] ; then
    export gcp_credentials=$GOOGLE_CREDENTIALS_CUSTOMIZED
    gcs_bucket="cockroach-customized-builds-artifacts-prod"
    gcr_repository="us-docker.pkg.dev/cockroach-cloud-images/cockroachdb-customized/${cockroach_archive_prefix}-customized"
  else
    export gcp_credentials=$GOOGLE_COCKROACH_CLOUD_IMAGES_COCKROACHDB_CREDENTIALS
    gcs_bucket="cockroach-builds-artifacts-prod"
    gcr_repository="us-docker.pkg.dev/cockroach-cloud-images/cockroachdb/${cockroach_archive_prefix}"
    # GCS uses different credentials for build artifact uploads
    gcs_credentials="$GCS_CREDENTIALS_PROD"
  fi
else
  build_name="${build_name}.dryrun"
  export gcp_credentials="$GOOGLE_COCKROACH_RELEASE_CREDENTIALS"
  gcs_bucket="cockroach-builds-artifacts-dryrun"
  gcr_repository="us.gcr.io/cockroach-release/${cockroach_archive_prefix}-test"
  gcs_credentials="$GCS_CREDENTIALS_DEV"
fi

cat << EOF

  build_name:             $build_name
  gcr_repository:         $gcr_repository
  release_build_match:    $release_build_match

EOF
tc_end_block "Variable Setup"


tc_start_block "Download and extract tarballs"
# For release builds, GCS artifact buckets and the container registry use
# different service accounts (GCS_CREDENTIALS_PROD vs
# GOOGLE_COCKROACH_CLOUD_IMAGES_COCKROACHDB_CREDENTIALS). For customized and
# dry-run builds, a single credential covers both. Use the GCS-specific
# credential when set, otherwise fall back to the unified one.
google_credentials="${gcs_credentials:-$gcp_credentials}"
log_into_gcloud

tmpdir=$(mktemp -d)
trap "rm -rf $tmpdir; remove_files_on_exit" EXIT

# Download and extract per-arch tarballs into a build context laid out as
# ${arch}/ subdirectories, matching what build/deploy/Dockerfile expects.
context="$tmpdir/context"
mkdir -p "$context"
cp build/deploy/Dockerfile "$context/Dockerfile"

for platform in linux-amd64 linux-arm64 linux-s390x; do
  arch="${platform#linux-}"
  archive="${cockroach_archive_prefix}-${build_name}.${platform}.tgz"
  gsutil -o 'GSUtil:num_retries=5' cp "gs://$gcs_bucket/$archive" "$tmpdir/$archive"
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

# FIPS is amd64-only; prepare its own build context.
fips_context="$tmpdir/fips-context"
mkdir -p "$fips_context/amd64"
cp build/deploy/Dockerfile "$fips_context/Dockerfile"
fips_archive="${cockroach_archive_prefix}-${build_name}.linux-amd64-fips.tgz"
gsutil -o 'GSUtil:num_retries=5' cp "gs://$gcs_bucket/$fips_archive" "$tmpdir/$fips_archive"
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
tc_end_block "Download and extract tarballs"


tc_start_block "Build and push multi-arch docker image"
docker_login_gcr "$gcr_repository" "$gcp_credentials"

docker buildx rm "build-artifacts-builder-$$" 2>/dev/null || true
docker buildx create --name "build-artifacts-builder-$$" --use
cleanup_buildx() { docker buildx rm "build-artifacts-builder-$$" || true; }
trap "cleanup_buildx; rm -rf $tmpdir; remove_files_on_exit" EXIT

gcr_tag="${gcr_repository}:${build_name}"
docker buildx build --pull --push --no-cache \
  --platform linux/amd64,linux/arm64,linux/s390x \
  --tag "$gcr_tag" "$context"
tc_end_block "Build and push multi-arch docker image"


tc_start_block "Build and push FIPS docker image"
gcr_tag_fips="${gcr_repository}:${build_name}-fips"
docker buildx build --pull --push --no-cache \
  --platform linux/amd64 \
  --tag "$gcr_tag_fips" "$fips_context"
tc_end_block "Build and push FIPS docker image"


tc_start_block "Verify docker images"
error=0
for arch in amd64 arm64 s390x; do
    tc_start_block "Verify $gcr_tag on $arch"
    if ! verify_docker_image "$gcr_tag" "linux/$arch" "$BUILD_VCS_NUMBER" "$build_name" false "$telemetry_disabled"; then
      error=1
    fi
    tc_end_block "Verify $gcr_tag on $arch"
done

tc_start_block "Verify FIPS docker image"
if ! verify_docker_image "$gcr_tag_fips" "linux/amd64" "$BUILD_VCS_NUMBER" "$build_name" true "$telemetry_disabled"; then
  error=1
fi
tc_end_block "Verify FIPS docker image"

if [ $error = 1 ]; then
  echo "ERROR: Docker image verification failed, see logs above"
  exit 1
fi
tc_end_block "Verify docker images"
