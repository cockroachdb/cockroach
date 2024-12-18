#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname $(dirname "${0}"))))))"
source "$dir/release/teamcity-support.sh"
source "$dir/shlib.sh"


tc_start_block "Variable Setup"
version=$(grep -v "^#" "$dir/../pkg/build/version.txt" | head -n1)
if [[ $version == *"-"* ]]; then
  echo "Pushing pre-release versions to Red Hat is not implemented (there is no unstable repository for them to live)"
  exit 0
fi

PUBLISH_LATEST=
if is_latest "$version"; then
  PUBLISH_LATEST=true
fi

# Hard coded release number used only by the RedHat images
rhel_release=1
rhel_project_id=5e61ea74fe2231a0c2860382
rhel_registry="quay.io"
rhel_registry_username="redhat-isv-containers+${rhel_project_id}-robot"
rhel_repository="${rhel_registry}/redhat-isv-containers/$rhel_project_id"
dockerhub_repository="cockroachdb/cockroach"

if ! [[ -z "${DRY_RUN}" ]] ; then
  version="${version}-dryrun"
  dockerhub_repository="cockroachdb/cockroach-misc"
fi
tc_end_block "Variable Setup"

tc_start_block "Configure docker"
echo "${QUAY_REGISTRY_KEY}" | docker login --username $rhel_registry_username --password-stdin $rhel_registry
tc_end_block "Configure docker"

tc_start_block "Rebuild docker image"
sed \
  -e "s,@repository@,${dockerhub_repository},g" \
  -e "s,@tag@,${version},g" \
  build/deploy-redhat/Dockerfile.in > build/deploy-redhat/Dockerfile

cat build/deploy-redhat/Dockerfile

docker build --no-cache \
  --pull \
  --label release=$rhel_release \
  --label version=$version \
  --tag="${rhel_repository}:${version}" \
  build/deploy-redhat
tc_end_block "Rebuild docker image"

tc_start_block "Push RedHat docker image"
retry docker push "${rhel_repository}:${version}"
tc_end_block "Push RedHat docker image"

tc_start_block "Tag docker image as latest"
if [[ -n "${PUBLISH_LATEST}" ]]; then
  docker tag "${rhel_repository}:${version}" "${rhel_repository}:latest"
  retry docker push "${rhel_repository}:latest"
else
  echo "Not required"
fi
tc_end_block "Tag docker images as latest"

tc_start_block "Run preflight"
mkdir -p artifacts
docker run \
  --rm \
  --security-opt=label=disable \
  --env PFLT_LOGLEVEL=trace \
  --env PFLT_ARTIFACTS=/artifacts \
  --env PFLT_LOGFILE=/artifacts/preflight.log \
  --env PFLT_CERTIFICATION_PROJECT_ID="$rhel_project_id" \
  --env PFLT_PYXIS_API_TOKEN="$REDHAT_API_TOKEN" \
  --env PFLT_DOCKERCONFIG=/temp-authfile.json \
  --env DOCKER_CONFIG=/tmp/docker \
  -v "$PWD/artifacts:/artifacts" \
  -v ~/.docker/config.json:/temp-authfile.json:ro \
  -v ~/.docker/config.json:/tmp/docker/config.json:ro \
  quay.io/opdev/preflight:stable check container \
  "${rhel_repository}:${version}" --submit
tc_end_block "Run preflight"
