#!/usr/bin/env bash

set -ex

mkdir -p artifacts

# Build the cockroach binary (which uses docker) before hopping into docker for
# the nightly-workload.sh script.
pkg/acceptance/prepare.sh

docker run \
    --workdir=/go/src/github.com/cockroachdb/cockroach \
    --volume="${GOPATH%%:*}/src":/go/src \
    --env="AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}" \
    --env="AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}" \
    --env="GOOGLE_CREDENTIALS=${GOOGLE_CREDENTIALS}" \
    --env="TC_BUILD_ID=${TC_BUILD_ID}" \
    --rm \
    cockroachdb/builder:20171004-085709 ./build/nightly-workload.sh
