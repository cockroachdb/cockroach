#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

this_dir="$(cd "$(dirname "${0}")"; pwd)"
toplevel="$(dirname $(dirname $(dirname $(dirname $this_dir))))"

mkdir -p "${toplevel}"/artifacts

# note: the Docker image should match the base image of `us-east1-docker.pkg.dev/crl-ci-images/cockroach/bazel`.
docker run --rm -i ${tty-} -v $this_dir:/bootstrap \
       -v "${toplevel}"/artifacts:/artifacts \
       us-east1-docker.pkg.dev/crl-docker-sync/docker-io/library/ubuntu:focal-20210119 /bootstrap/perform-build.sh
