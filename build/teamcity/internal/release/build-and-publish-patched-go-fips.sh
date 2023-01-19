#!/usr/bin/env bash

set -euo pipefail

google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"
dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"  # for log_into_gcloud
log_into_gcloud

set -x

this_dir="$(cd "$(dirname "${0}")"; pwd)"
toplevel="$(dirname $(dirname $(dirname $(dirname $this_dir))))"

mkdir -p "${toplevel}"/artifacts
# TODO: pin docker image version
DOCKER_IMAGE=registry.access.redhat.com/ubi8/go-toolset:latest

tc_start_block "Build Go toolchains (linux/amd64)" 
docker run --rm -i ${tty-} -v $this_dir/build-and-publish-patched-go:/bootstrap \
  -v "${toplevel}"/artifacts:/artifacts \
  --user root \
  --platform linux/amd64 \
  $DOCKER_IMAGE /bootstrap/impl-fips.sh
tc_end_block "Build Go toolchains (linux/amd64)"

tc_start_block "Build Go toolchains (linux/arm64)" 
docker run --rm -i ${tty-} -v $this_dir/build-and-publish-patched-go:/bootstrap \
  -v "${toplevel}"/artifacts:/artifacts \
  --user root \
  --platform linux/arm64 \
  $DOCKER_IMAGE /bootstrap/impl-fips.sh
tc_end_block "Build Go toolchains (linux/arm64)"

tc_start_block "Publish artifacts"
loc=$(date +%Y%m%d-%H%M%S)
for FILE in `find $root/artifacts -name '*.tar.gz'`; do
    BASE=$(basename $FILE)
    echo gsutil cp $FILE gs://public-bazel-artifacts/go-fips/$loc/$BASE
done
tc_end_block "Publish artifacts"

tc_end_block "Print checksums"
sha256sum $root/artifacts/*.tar.gz
tc_start_block "Print checksums"
