#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -xeuo pipefail

# When updating to a new Go version, update all of these variables.
GOVERS=1.23.6
GOCOMMIT=$(grep -v ^# /bootstrap/commit.txt | head -n1)
# We use this for bootstrapping (this is NOT re-published). Note the version
# matches the version we're publishing, although it doesn't technically have to.
GOLINUXLINK=https://go.dev/dl/go$GOVERS.linux-amd64.tar.gz
LINUXSHASUM=9379441ea310de000f33a4dc767bd966e72ab2826270e038e78b2c53c2e7802d

apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    apt-transport-https \
    ca-certificates \
    clang-10 \
    cmake \
    curl \
    git \
    gnupg2 \
    make \
    python-is-python3 \
    python3 \
    python3.8-venv

update-alternatives --install /usr/bin/clang clang /usr/bin/clang-10 100 \
    --slave /usr/bin/clang++ clang++ /usr/bin/clang++-10

curl -fsSL $GOLINUXLINK -o golang.tar.gz \
 && echo "$LINUXSHASUM  golang.tar.gz" | sha256sum -c - \
 && rm -rf /usr/local/go && tar -C /usr/local -xzf golang.tar.gz \
 && rm golang.tar.gz

PATH=$PATH:/usr/local/go/bin

# libtapi is required for later versions of MacOSX.
git clone https://github.com/tpoechtrager/apple-libtapi.git
cd apple-libtapi
git checkout a66284251b46d591ee4a0cb4cf561b92a0c138d8
./build.sh
./install.sh
cd ..
rm -rf apple-libtapi

curl -fsSL https://storage.googleapis.com/public-bazel-artifacts/toolchains/crosstool-ng/x86_64/20230906-034412/aarch64-unknown-linux-gnu.tar.gz -o aarch64-unknown-linux-gnu.tar.gz
echo 'f9b073774826747cf2a91514d5ab27e3ba7f0c7b63acaf80a5ed58c82b08fd44 aarch64-unknown-linux-gnu.tar.gz' | sha256sum -c -
curl -fsSL https://storage.googleapis.com/public-bazel-artifacts/toolchains/osxcross/x86_64/20220317-165434/x86_64-apple-darwin21.2.tar.gz -o x86_64-apple-darwin21.2.tar.gz
echo '751365dbfb5db66fe8e9f47fcf82cbbd7d1c176b79112ab91945d1be1d160dd5 x86_64-apple-darwin21.2.tar.gz' | sha256sum -c -
curl -fsSL https://storage.googleapis.com/public-bazel-artifacts/toolchains/crosstool-ng/x86_64/20230906-034412/x86_64-unknown-linux-gnu.tar.gz -o x86_64-unknown-linux-gnu.tar.gz
echo '5f79da0a9e580bc0a869ca32c2e5a21990676ec567aabf54ccc1dec4c3f2c827 x86_64-unknown-linux-gnu.tar.gz' | sha256sum -c -
curl -fsSL https://storage.googleapis.com/public-bazel-artifacts/toolchains/crosstool-ng/x86_64/20230906-034412/x86_64-w64-mingw32.tar.gz -o x86_64-w64-mingw32.tar.gz
echo '94e64e0e8de05706dfd5ab2f1fee6e7f75280e35b09b5628980805d27939b418 x86_64-w64-mingw32.tar.gz' | sha256sum -c -
curl -fsSL https://storage.googleapis.com/public-bazel-artifacts/toolchains/crosstool-ng/x86_64/20230906-034412/s390x-ibm-linux-gnu.tar.gz -o s390x-ibm-linux-gnu.tar.gz
echo '027d7d3b89d0c9745243610b9c12aa26f5605884b058934645cb344927228dab s390x-ibm-linux-gnu.tar.gz' | sha256sum -c -
echo *.tar.gz | xargs -n1 tar -xzf
rm *.tar.gz

mkdir -p /tmp/go$GOVERS
cd /tmp/go$GOVERS
git clone 'https://github.com/cockroachdb/go.git'
cd go
git checkout $GOCOMMIT
cd ..

CONFIGS="linux_amd64 linux_arm64 linux_s390x darwin_amd64 darwin_arm64 windows_amd64"

for CONFIG in $CONFIGS; do
    case $CONFIG in
        linux_amd64)
            CC_FOR_TARGET=/x-tools/x86_64-unknown-linux-gnu/bin/x86_64-unknown-linux-gnu-cc
            CXX_FOR_TARGET=/x-tools/x86_64-unknown-linux-gnu/bin/x86_64-unknown-linux-gnu-c++
        ;;
        linux_arm64)
            CC_FOR_TARGET=/x-tools/aarch64-unknown-linux-gnu/bin/aarch64-unknown-linux-gnu-cc
            CXX_FOR_TARGET=/x-tools/aarch64-unknown-linux-gnu/bin/aarch64-unknown-linux-gnu-c++
            ;;
        linux_s390x)
            CC_FOR_TARGET=/x-tools/s390x-ibm-linux-gnu/bin/s390x-ibm-linux-gnu-cc
            CXX_FOR_TARGET=/x-tools/s390x-ibm-linux-gnu/bin/s390x-ibm-linux-gnu-c++
        ;;
        darwin_amd64)
            CC_FOR_TARGET=/x-tools/x86_64-apple-darwin21.2/bin/x86_64-apple-darwin21.2-cc
            CXX_FOR_TARGET=/x-tools/x86_64-apple-darwin21.2/bin/x86_64-apple-darwin21.2-c++
            ;;
        darwin_arm64)
            CC_FOR_TARGET=/x-tools/x86_64-apple-darwin21.2/bin/aarch64-apple-darwin21.2-cc
            CXX_FOR_TARGET=/x-tools/x86_64-apple-darwin21.2/bin/aarch64-apple-darwin21.2-c++
        ;;
        windows_amd64)
            CC_FOR_TARGET=/x-tools/x86_64-w64-mingw32/bin/x86_64-w64-mingw32-cc
            CXX_FOR_TARGET=/x-tools/x86_64-w64-mingw32/bin/x86_64-w64-mingw32-c++
        ;;
    esac
    GOOS=$(echo $CONFIG | cut -d_ -f1)
    GOARCH=$(echo $CONFIG | cut -d_ -f2)
    cd go/src
    if [ $GOOS == darwin ]; then
        export LD_LIBRARY_PATH=/x-tools/x86_64-apple-darwin21.2/lib
    fi
    GOOS=$GOOS GOARCH=$GOARCH CC=clang CXX=clang++ CC_FOR_TARGET=$CC_FOR_TARGET CXX_FOR_TARGET=$CXX_FOR_TARGET \
               GOROOT_BOOTSTRAP=$(go env GOROOT) CGO_ENABLED=1 ./make.bash
    if [ $GOOS == darwin ]; then
        unset LD_LIBRARY_PATH
    fi
    cd ../..
    rm -rf /tmp/go$GOVERS/go/pkg/${GOOS}_$GOARCH/cmd
    for OTHER_CONFIG in $CONFIGS; do
        if [ $CONFIG != $OTHER_CONFIG ]; then
            rm -rf /tmp/go$GOVERS/go/pkg/$OTHER_CONFIG
        fi
    done
    if [ $CONFIG != linux_amd64 ]; then
        rm go/bin/go go/bin/gofmt
        mv go/bin/${GOOS}_$GOARCH/* go/bin
        rm -r go/bin/${GOOS}_$GOARCH
    fi
    tar cf - go | gzip -9 > /artifacts/go$GOVERS.$GOOS-$GOARCH.tar.gz
    rm -rf go/bin
done

sha256sum /artifacts/*.tar.gz
