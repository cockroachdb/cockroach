#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


#!/usr/bin/env bash
set -euxo pipefail

# The first and only parameter is the name of the architecture the image is being built for,
# either amd64 or arm64.
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
    ;;
esac

build_arch=${1:-amd64}

bazel build //pkg/cmd/cockroach //c-deps:libgeos --config $CROSSCONFIG --jobs 50 $(./build/github/engflow-args.sh)

# The Dockerfile expects per-arch files under ${TARGETARCH}/ subdirectories.
mkdir -p "build/deploy/${build_arch}"
cp _bazel/bin/pkg/cmd/cockroach/cockroach_/cockroach "build/deploy/${build_arch}/"
LIBDIR=$(bazel info execution_root --config $CROSSCONFIG)/external/$ARCHIVEDIR/lib
cp $LIBDIR/libgeos.so "build/deploy/${build_arch}/"
cp $LIBDIR/libgeos_c.so "build/deploy/${build_arch}/"
cp build/deploy/cockroach.sh "build/deploy/${build_arch}/"
cp LICENSE licenses/THIRD-PARTY-NOTICES.txt "build/deploy/${build_arch}/"

chmod 755 "build/deploy/${build_arch}/cockroach"

docker_image_tar_name="cockroach-docker-image.tar"

docker_tag="cockroachdb/cockroach-ci"

# We have to always pull here because this runner may have been used to build
# a different architecture's docker image. If that's the case, the cache will
# return the cached version of the UBI base image (which will be for the wrong
# architecture), then build will use it and fail because it's for the wrong
# architecture. The cache is really stupid, in other words.
docker build \
  --no-cache \
  --platform=linux/${build_arch} \
  --tag="$docker_tag" \
  --memory 30g \
  --memory-swap -1 \
  --pull \
  build/deploy

bazel test \
  //pkg/testutils/docker:docker_test \
  --config=crosslinux \
  --test_timeout=3000 \
  --remote_download_minimal \
  --jobs 50 $(./build/github/engflow-args.sh) --build_event_binary_file=bes.bin
