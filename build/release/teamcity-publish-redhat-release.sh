#!/usr/bin/env bash

set -euxo pipefail

source "$(dirname "${0}")/teamcity-support.sh"
source "$(dirname "${0}")/../shlib.sh"

tc_start_block "Sanity Check"
# Make sure that the version matches the TeamCity branch name. The TeamCity
# branch name becomes available only after the new tag is pushed to GitHub by
# the `teamcity-publish-release.sh` script.
# In the future, when this script becomes a part of the automated process, we
# may need to change this check to match the tag used by the process.
if [[ $TC_BUILD_BRANCH != ${NAME} ]]; then
  echo "Release name \"$NAME\" cannot be built using \"$TC_BUILD_BRANCH\""
  exit 1
fi
if ! [[ -z "$PRE_RELEASE" ]]; then
  echo "Pushing pre-release versions to Red Hat is not implemented (there is no unstable repository for them to live)"
  exit 0
fi
tc_end_block "Sanity Check"


tc_start_block "Variable Setup"
# Accept only X.Y.Z versions, because we don't publish images for alpha versions
build_name="$(echo "${NAME}" | grep -E -o '^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$')"
#                                             ^major           ^minor           ^patch
if [[ -z "$build_name" ]] ; then
    echo "Unsupported version \"${NAME}\". Must be of the format \"vMAJOR.MINOR.PATCH\"."
    exit 0
fi
# Hard coded release number used only by the RedHat images
rhel_release=1
rhel_registry="scan.connect.redhat.com"
rhel_repository="${rhel_registry}/p194808216984433e18e6e90dd859cb1ea7c738ec50/cockroach"
dockerhub_repository="cockroachdb/cockroach"

if ! [[ -z "${DRY_RUN}" ]] ; then
  build_name="${build_name}-dryrun"
  dockerhub_repository="cockroachdb/cockroach-misc"
fi
tc_end_block "Variable Setup"

tc_start_block "Configure docker"
docker_login_with_redhat
tc_end_block "Configure docker"

tc_start_block "Rebuild docker image"
sed \
  -e "s,@repository@,${dockerhub_repository},g" \
  -e "s,@tag@,${build_name},g" \
  build/deploy-redhat/Dockerfile.in > build/deploy-redhat/Dockerfile

cat build/deploy-redhat/Dockerfile

docker build --no-cache \
  --label release=$rhel_release \
  --tag=${rhel_repository}:${build_name} \
  build/deploy-redhat
tc_end_block "Rebuild docker image"

tc_start_block "Push RedHat docker image"
retry docker push "${rhel_repository}:${build_name}"
tc_end_block "Push RedHat docker image"
