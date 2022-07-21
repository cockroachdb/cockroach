#!/usr/bin/env bash

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
# note: the Docker image should match the base image of
# `cockroachdb/builder` and `cockroachdb/bazel`.
docker run --rm -i ${tty-} -v $this_dir/build-and-publish-patched-go:/bootstrap \
       -v "${toplevel}"/artifacts:/artifacts \
       ubuntu:focal-20210119 /bootstrap/impl.sh
tc_end_block "Build Go toolchains"

tc_start_block "Publish artifacts"
loc=$(date +%Y%m%d-%H%M%S)
for FILE in `find $root/artifacts -name '*.tar.gz'`; do
    BASE=$(basename $FILE)
    gsutil cp $FILE gs://public-bazel-artifacts/go/$loc/$BASE
done
tc_end_block "Publish artifacts"
