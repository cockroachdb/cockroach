#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"
source "$dir/release/teamcity-support.sh"

tc_start_block "Variable Setup"
version=$(grep -v "^#" "$dir/../pkg/build/version.txt" | head -n1)
if [[ -z "${DRY_RUN}" ]] ; then
  gcr_staged_repository="us-docker.pkg.dev/releases-prod/cockroachdb-staged-releases/cockroach"
  gcr_staged_credentials="$GCS_CREDENTIALS_PROD"
  gcr_repository="us-docker.pkg.dev/cockroach-cloud-images/cockroachdb/cockroach"
  gcr_credentials="$GOOGLE_COCKROACH_CLOUD_IMAGES_COCKROACHDB_CREDENTIALS"
else
  gcr_staged_repository="us-docker.pkg.dev/releases-dev-356314/cockroachdb-staged-releases/cockroach"
  gcr_staged_credentials="$GCS_CREDENTIALS_DEV"
  gcr_repository="us-docker.pkg.dev/releases-dev-356314/cockroachdb-staged-releases/cockroach-cloud-only"
  gcr_credentials="$GCS_CREDENTIALS_DEV"
fi
tc_end_block "Variable Setup"


docker_login_gcr "$gcr_staged_repository" "$gcr_staged_credentials"
docker pull "${gcr_staged_repository}:amd64-${version}"
docker pull "${gcr_staged_repository}:arm64-${version}"

cloud_release=
for i in $(seq 1 10); do
  maybe_manifest="${gcr_repository}:$version-cloudonly.$i"
  if ! docker manifest inspect "$maybe_manifest"; then
    echo "$maybe_manifest not found, assuming this is what we need"
    cloud_release="$i"
    break
  else
    echo "$maybe_manifest already exist, trying next..."
  fi
done

if [[ -z $cloud_release ]]; then
  echo "cannot find available cloud_release, did we generate 10 of them?"
  exit 1
fi

version_cloudonly="${version}-cloudonly.${cloud_release}"
manifest="${gcr_repository}:${version_cloudonly}"

docker_login_gcr "$gcr_repository" "$gcr_credentials"

docker tag \
  "${gcr_staged_repository}:amd64-${version}" \
  "${gcr_repository}:amd64-${version_cloudonly}"
docker tag \
  "${gcr_staged_repository}:arm64-${version}" \
  "${gcr_repository}:arm64-${version_cloudonly}"

docker push "${gcr_repository}:amd64-${version_cloudonly}"
docker push "${gcr_repository}:arm64-${version_cloudonly}"

docker manifest rm "$manifest" || :
docker manifest create \
  "$manifest" \
  "${gcr_repository}:amd64-${version_cloudonly}" \
  "${gcr_repository}:arm64-${version_cloudonly}"

docker manifest push "$manifest" 
echo "==========="
echo "published to"
echo "$manifest"
echo "==========="

tc_start_block "Verify docker images"
error=0
for arch in amd64 arm64; do
    tc_start_block "Verify $manifest on $arch"
    if ! verify_docker_image "$manifest" "linux/$arch" "$BUILD_VCS_NUMBER" "$version" false; then
      error=1
    fi
    tc_end_block "Verify $manifest on $arch"
done

if [ $error = 1 ]; then
  echo "ERROR: Docker image verification failed, see logs above"
  exit 1
fi
tc_end_block "Verify docker images"

tc_start_block "Metadata"
timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
metadata_file="artifacts/metadata.json"
mkdir -p artifacts
cat > "$metadata_file" << EOF
{
  "sha": "$BUILD_VCS_NUMBER",
  "timestamp": "$timestamp",
  "tag": "$TC_BUILD_BRANCH",
  "version": "$version",
  "cloud_release": "$cloud_release",
  "image": "$manifest"
}
EOF
# Run jq to pretty print and validate JSON
jq . "$metadata_file"
tc_end_block "Metadata"
