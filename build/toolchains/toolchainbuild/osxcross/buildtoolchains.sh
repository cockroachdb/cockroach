#!/usr/bin/env bash

set -euxo pipefail

this_dir="$(cd "$(dirname "${0}")"; pwd)"
toplevel="$(dirname $(dirname $(dirname $(dirname $this_dir))))"

mkdir -p "${toplevel}"/artifacts

# note: the Docker image should match the base image of `us-east1-docker.pkg.dev/crl-ci-images/cockroach/bazel`.
# We use a docker image mirror to avoid pulling from 3rd party repos, which sometimes have reliability issues.
# See https://cockroachlabs.atlassian.net/wiki/spaces/devinf/pages/3462594561/Docker+image+sync for the details.
docker run --rm -i ${tty-} -v $this_dir:/bootstrap \
       -v "${toplevel}"/artifacts:/artifacts \
       us-east1-docker.pkg.dev/crl-docker-sync/docker-io/library/ubuntu:noble /bootstrap/perform-build.sh
