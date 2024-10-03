#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    apt-transport-https \
    autoconf \
    bzip2 \
    ca-certificates \
    clang-10 \
    cmake \
    curl \
    file \
    flex \
    gawk \
    git \
    gnupg2 \
    gperf \
    help2man \
    libncurses-dev \
    libssl-dev \
    libtool-bin \
    libxml2-dev \
    make \
    patch \
    patchelf \
    python \
    texinfo \
    xz-utils \
    unzip \
    zlib1g \
    zlib1g-dev \
 && update-alternatives --install /usr/bin/clang clang /usr/bin/clang-10 100 \
    --slave /usr/bin/clang++ clang++ /usr/bin/clang++-10 \
 && apt-get clean

# libtapi is required for later versions of MacOSX.
git clone https://github.com/tpoechtrager/apple-libtapi.git \
    && cd apple-libtapi \
    && git checkout a66284251b46d591ee4a0cb4cf561b92a0c138d8 \
    && ./build.sh \
    && ./install.sh \
    && cd .. \
    && rm -rf apple-libtapi

# Install osxcross. This needs the min supported osx version (we bump that
# whenever Go does, in which case the builder image stops working). The SDK
# can be generated from Xcode by following
# https://github.com/tpoechtrager/osxcross#packaging-the-sdk.
#
# See https://en.wikipedia.org/wiki/Uname for the right suffix in the `mv` step
# below. For example, Yosemite is 10.10 and has kernel release (uname -r)
# 14.0.0. Similar edits are needed in mkrelease.sh.
#
# The commit here is `master` as of the time of this writing, plus a patch to
# consume the latest MacOS SDK version. When that PR is merged
# (https://github.com/tpoechtrager/osxcross/pull/330) we can point back to the upstream
# `master`.
git clone https://github.com/cockroachdb/osxcross.git \
 && (cd osxcross && git checkout 60076247aaa3c65953043ceaf8b773223f280315) \
 && (cd osxcross/tarballs && curl -sfSL https://cockroach-builder-assets.s3.amazonaws.com/MacOSX12.1.sdk.tar.xz -O) \
 && echo "8927a6a2915f0a94c7b6f8c7f29b72081cc20659a05c4996830d1d9c7f754d3a osxcross/tarballs/MacOSX12.1.sdk.tar.xz" | sha256sum -c - \
 && OSX_VERSION_MIN=10.15 PORTABLE=1 UNATTENDED=1 osxcross/build.sh \
 && mkdir /x-tools \
 && mv osxcross/target /x-tools/x86_64-apple-darwin21.2 \
 && rm -rf osxcross

# Bundle artifacts
bundle() {
    filename=/artifacts/$(echo $1 | rev | cut -d/ -f1 | rev).tar.gz
    tar -czf $filename $1
    # Print the sha256 for debugging purposes.
    shasum -a 256 $filename
}
bundle /x-tools/x86_64-apple-darwin21.2
