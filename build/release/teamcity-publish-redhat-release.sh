#!/usr/bin/env bash

set -euxo pipefail

source "$(dirname "${0}")/teamcity-support.sh"
source "$(dirname "${0}")/../shlib.sh"

tc_start_block "Variable Setup"
if ! [[ -z "$PRE_RELEASE" ]]; then
  echo "Pushing pre release versions to RedHat is not implemented"
  exit 0
fi

# Accept only X.Y.Z versions, because we don't publish images for alpha versions
build_name="$(echo "${NAME}" | grep -E -o '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$')"
#                                             ^major           ^minor           ^patch
# Do not use the dry run version
upstream_build_name="$build_name"

# Hard coded release number used only by the RedHat images
rhel_release=1
rhel_registry="scan.connect.redhat.com"
rhel_repository="${rhel_registry}/p194808216984433e18e6e90dd859cb1ea7c738ec50/cockroach"


if [[ -z "$build_name" ]] ; then
    echo "Invalid NAME \"${NAME}\". Must be of the format \"vMAJOR.MINOR.PATCH\"."
    exit 1
fi

if ! [[ -z "${DRY_RUN}" ]] ; then
  build_name="${build_name}-dryrun"
fi
tc_end_block "Variable Setup"

tc_start_block "Configure docker"
docker_login_with_redhat
tc_end_block "Configure docker"

tc_start_block "Rebuild docker image"
sed -e "s/@build_name@/${upstream_build_name}/g" build/deploy-redhat/Dockerfile.in > build/deploy-redhat/Dockerfile

cat build/deploy-redhat/Dockerfile

docker build --no-cache \
  --label release=$rhel_release \
  --tag=${rhel_repository}:${build_name} \
  build/deploy-redhat
tc_end_block "Rebuild docker image"

tc_start_block "Push RedHat docker image"
retry docker push "${rhel_repository}:${build_name}"
tc_end_block "Push RedHat docker image"
