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
docker run --rm -i ${tty-} -v $this_dir/build-and-publish-patched-go:/bootstrap \
       -v "${toplevel}"/artifacts:/artifacts \
       ubuntu:focal /bootstrap/impl.sh
tc_end_block "Build Go toolchains"

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
