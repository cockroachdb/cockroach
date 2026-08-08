#!/usr/bin/env bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# Builds the slim CockroachDB Docker image (see build/deploy-slim/Dockerfile).
# Parallel to build/github/docker-image.sh; does not replace it.
#
# The slim image is intended for CI use cases that want fast pulls and do not
# need the DB Console UI, interactive shell entrypoint, or kubectl-cp
# helpers. It uses a distroless base and a stripped cockroach-short binary.

set -euxo pipefail

# The first and only parameter is the name of the architecture the image is
# being built for, either amd64 or arm64.
case $1 in
    amd64)
        CROSSCONFIG=crosslinux
        ARCHIVEDIR=archived_cdep_libgeos_linux
    ;;
    arm64)
        CROSSCONFIG=crosslinuxarm
        ARCHIVEDIR=archived_cdep_libgeos_linuxarm
    ;;
    *)
        echo 'expected one argument, either amd64 or arm64'
        exit 1
    ;;
esac

build_arch=${1:-amd64}

bazel build //pkg/cmd/cockroach-short //c-deps:libgeos \
  --config $CROSSCONFIG --jobs 50 $(./build/github/engflow-args.sh)

# The Dockerfile expects per-arch files under ${TARGETARCH}/ subdirectories.
# The binary is renamed from cockroach-short to cockroach so that the in-image
# command matches the standard image (docker run ... cockroach start-...).
mkdir -p "build/deploy-slim/${build_arch}"
cp _bazel/bin/pkg/cmd/cockroach-short/cockroach-short_/cockroach-short \
   "build/deploy-slim/${build_arch}/cockroach"
LIBDIR=$(bazel info execution_root --config $CROSSCONFIG)/external/$ARCHIVEDIR/lib
cp $LIBDIR/libgeos.so "build/deploy-slim/${build_arch}/"
cp $LIBDIR/libgeos_c.so "build/deploy-slim/${build_arch}/"
cp LICENSE licenses/THIRD-PARTY-NOTICES.txt "build/deploy-slim/${build_arch}/"

chmod 755 "build/deploy-slim/${build_arch}/cockroach"

docker_tag="cockroachdb/cockroach-slim"

# --no-cache + --pull mirrors docker-image.sh: the runner may have been used
# for a different architecture's build, and the cache can serve the wrong-arch
# base image.
docker build \
  --no-cache \
  --platform=linux/${build_arch} \
  --tag="$docker_tag" \
  --memory 30g \
  --memory-swap -1 \
  --pull \
  build/deploy-slim

bazel test \
  //pkg/testutils/docker-slim:docker-slim_test \
  --config=crosslinux \
  --test_timeout=3000 \
  --remote_download_minimal \
  --jobs 50 $(./build/github/engflow-args.sh) --build_event_binary_file=bes.bin
