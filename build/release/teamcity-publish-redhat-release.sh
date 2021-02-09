#!/usr/bin/env bash

set -euxo pipefail

source "$(dirname "${0}")/teamcity-support.sh"
source "$(dirname "${0}")/../shlib.sh"

tc_start_block "Variable Setup"
source "$(dirname "${0}")/release-support.sh"
tc_end_block "Variable Setup"

tc_start_block "Configure docker"
configure_docker_creds
docker_login_with_google
docker_login
tc_end_block "Configure docker"

tc_start_block "Rebuild docker image"
if [[ -z "${DRY_RUN}" ]] ; then
  rhel_repository="scan.connect.redhat.com/p194808216984433e18e6e90dd859cb1ea7c738ec50/cockroach"
  # TODO(rail): make sure that using our FROM: is acceptable by rhel
  sed -e "s/@build_name@/${build_name}/g" build/redhat/Dockerfile.in > build/redhat/Dockerfile
  docker build --no-cache \
    --label version=$version \
    --label release=$release \
    --tag=${rhel_repository}:${build_name} \
    build/deploy-redhat
  # TODO(rail): docker push
else
  # TODO(rail): create a dry-run env for rhel
    echo "Dry run for RedHat is not implemented"
    exit 1
fi
tc_end_block "Rebuild docker image"

tc_start_block "Verify docker image"
# verify the image
# version and release labels
# TODO(rail): make sure jq is available on the build agents
ver="$(docker image inspect ${rhel_repository}:{$build_name} | jq -r '.[0].Config.Labels.version')"
rel="$(docker image inspect ${rhel_repository}:{$build_name} | jq -r '.[0].Config.Labels.release')"
if [ "$ver" != "$version" -o "$rel" != "$release" ]; then
    echo "Expected labels version:$version release:$release, got version:$ver release:$rel"
    exit 1
fi

# Make sure we have less than 40 layers (per RedHat requirements)
layers="$(docker history -q ${rhel_repository}:{$build_name} | wc -l | sed -e 's/ //g')"
if [ $layers -gt 40 ]; then
    echo "Expected less than 40 layers, got $layers"
    exit 1
fi


tc_start_block "Push RedHat docker image"
retry docker push "${rhel_repository}:${build_name}"
tc_end_block "Push RedHat docker image"
