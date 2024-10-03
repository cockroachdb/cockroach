#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail

google_credentials="$GOOGLE_EPHEMERAL_CREDENTIALS"
dir="$(dirname $(dirname $(dirname $(dirname "${0}"))))"
source "$dir/teamcity-support.sh"  # for log_into_gcloud
log_into_gcloud

set -x

tc_start_block "Build Go toolchains"
this_dir="$(cd "$(dirname "${0}")"; pwd)"
toplevel="$(dirname $(dirname $(dirname $(dirname $this_dir))))"

mkdir -p "${toplevel}"/artifacts
# We use a docker image mirror to avoid pulling from 3rd party repos, which sometimes have reliability issues.
# See https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/3462594561/Docker+image+sync for the details.
docker run --rm -i ${tty-} -v $this_dir/build-and-publish-patched-go:/bootstrap \
       -v "${toplevel}"/artifacts:/artifacts \
       us-east1-docker.pkg.dev/crl-docker-sync/docker-io/library/ubuntu:focal /bootstrap/impl.sh
tc_end_block "Build Go toolchains"

tc_start_block "Build FIPS Go toolchains (linux/amd64)"
# UBI 9 image with Go toolchain installed. The same image Red Hat uses for their CI.
UBI_DOCKER_IMAGE=registry.access.redhat.com/ubi9/go-toolset:1.18
# FIPS Go toolchain version has a 'fips' suffix, so there should be no name collision
docker run --rm -i ${tty-} -v "$this_dir/build-and-publish-patched-go:/bootstrap" \
  -v "${toplevel}"/artifacts:/artifacts \
  --user root \
  --platform linux/amd64 \
  $UBI_DOCKER_IMAGE \
  /bootstrap/impl-fips.sh
tc_end_block "Build FIPS Go toolchains (linux/amd64)"

tc_start_block "Publish artifacts"
loc=$(date +%Y%m%d-%H%M%S)
echo $loc > "${toplevel}"/artifacts/TIMESTAMP.txt
for FILE in `find $root/artifacts -name '*.tar.gz'`; do
    BASE=$(basename $FILE)
    if [[ "$BASE" != *"darwin"* ]]; then
        gsutil cp $FILE gs://public-bazel-artifacts/go/$loc/$BASE
    fi
done
tc_end_block "Publish artifacts"
