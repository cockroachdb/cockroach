#!/usr/bin/env bash

set -xeuo pipefail

# When updating to a new Go version, update all of these variables.
GOVERS=1.19.4
FIPS_PATCHES_RELEASE=1
GOLINK=https://go.dev/dl/go$GOVERS.src.tar.gz
SRCSHASUM=eda74db4ac494800a3e66ee784e495bfbb9b8e535df924a8b01b1a8028b7f368
# We mirror the upstream freebsd because we don't have a cross-compiler targeting it.
GOFREEBSDLINK=https://go.dev/dl/go$GOVERS.freebsd-amd64.tar.gz
FREEBSDSHASUM=84489ebb63f1757b79574d7345c647bd40bc6414cecb868c93e24476c2d2b9b6
# We mirror the upstream darwin/arm64 binary because we don't have code-signing yet.
GODARWINARMLINK=https://go.dev/dl/go$GOVERS.darwin-arm64.tar.gz
DARWINARMSHASUM=bb3bc5d7655b9637cfe2b5e90055dee93b0ead50e2ffd091df320d1af1ca853f
FIPS_PATCHES_URL=https://github.com/golang-fips/go/archive/refs/tags/go${GOVERS}-${FIPS_PATCHES_RELEASE}-openssl-fips.tar.gz
FIPS_PATCHES_SHASUM=d87a47deaf4ce70a9f5395c3afdd8f2b69a305638c0eb9ccf95a6b7d5df32f37

apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    apt-transport-https \
    ca-certificates \
    clang-10 \
    cmake \
    curl \
    git \
    gnupg2 \
    golang \
    make \
    patch \
    libssl-dev \
    libc6-dev-amd64-cross \
    libc6-dev-arm64-cross \
    python-is-python3 \
    python3 \
    python3.8-venv

update-alternatives --install /usr/bin/clang clang /usr/bin/clang-10 100 \
    --slave /usr/bin/clang++ clang++ /usr/bin/clang++-10

# curl -fsSL $GOFREEBSDLINK -o /artifacts/go$GOVERS.freebsd-amd64.tar.gz
# echo "$FREEBSDSHASUM  /artifacts/go$GOVERS.freebsd-amd64.tar.gz" | sha256sum -c -
# curl -fsSL $GODARWINARMLINK -o /artifacts/go$GOVERS.darwin-arm64.tar.gz
# echo "$DARWINARMSHASUM  /artifacts/go$GOVERS.darwin-arm64.tar.gz" | sha256sum -c -
#
# libtapi is required for later versions of MacOSX.
# git clone https://github.com/tpoechtrager/apple-libtapi.git
# cd apple-libtapi
# git checkout a66284251b46d591ee4a0cb4cf561b92a0c138d8
# ./build.sh
# ./install.sh
# cd ..
# rm -rf apple-libtapi

curl -fsSL https://storage.googleapis.com/public-bazel-artifacts/toolchains/crosstool-ng/x86_64/20220711-205918/aarch64-unknown-linux-gnu.tar.gz -o aarch64-unknown-linux-gnu.tar.gz
echo '58407f1f3ed490bd0a0a500b23b88503fbcc25f0f69a0b7f8a3e8e7b9237341b aarch64-unknown-linux-gnu.tar.gz' | sha256sum -c -
curl -fsSL https://storage.googleapis.com/public-bazel-artifacts/toolchains/osxcross/x86_64/20220317-165434/x86_64-apple-darwin21.2.tar.gz -o x86_64-apple-darwin21.2.tar.gz
echo '751365dbfb5db66fe8e9f47fcf82cbbd7d1c176b79112ab91945d1be1d160dd5 x86_64-apple-darwin21.2.tar.gz' | sha256sum -c -
curl -fsSL https://storage.googleapis.com/public-bazel-artifacts/toolchains/crosstool-ng/x86_64/20220711-205918/x86_64-unknown-linux-gnu.tar.gz -o x86_64-unknown-linux-gnu.tar.gz
echo '8b0c246c3ebd02aceeb48bb3d70c779a1503db3e99be332ac256d4f3f1c22d47 x86_64-unknown-linux-gnu.tar.gz' | sha256sum -c -
curl -fsSL https://storage.googleapis.com/public-bazel-artifacts/toolchains/crosstool-ng/x86_64/20220711-205918/x86_64-w64-mingw32.tar.gz -o x86_64-w64-mingw32.tar.gz
echo 'b87814aaeed8c68679852029de70cee28f96c352ed31c4c520e7bee55999b1c6 x86_64-w64-mingw32.tar.gz' | sha256sum -c -
echo *.tar.gz | xargs -n1 tar -xzf
rm *.tar.gz

curl -fsSL $FIPS_PATCHES_URL -o fips-patches.tar.gz
echo "$FIPS_PATCHES_SHASUM  fips-patches.tar.gz" | sha256sum -c -
mkdir -p /tmp/fips-patches
tar -C /tmp/fips-patches --strip-components 1 -xzf fips-patches.tar.gz
rm fips-patches.tar.gz

curl -fsSL $GOLINK -o golang.tar.gz
echo "$SRCSHASUM  golang.tar.gz" | sha256sum -c -
mkdir -p /tmp/go$GOVERS
tar -C /tmp/go$GOVERS -xzf golang.tar.gz
rm golang.tar.gz
cd /tmp/go$GOVERS/go
# NB: we apply a patch to the Go runtime to keep track of running time on a
# per-goroutine basis. See #82356 and #82625.
git apply /bootstrap/diff.patch
cd ..

for CONFIG in linux_amd64 linux_arm64 darwin_amd64 windows_amd64; do
    # TODO: enable me
    continue
    case $CONFIG in
        linux_amd64)
            CC_FOR_TARGET=/x-tools/x86_64-unknown-linux-gnu/bin/x86_64-unknown-linux-gnu-cc
            CXX_FOR_TARGET=/x-tools/x86_64-unknown-linux-gnu/bin/x86_64-unknown-linux-gnu-c++
        ;;
        linux_arm64)
            CC_FOR_TARGET=/x-tools/aarch64-unknown-linux-gnu/bin/aarch64-unknown-linux-gnu-cc
            CXX_FOR_TARGET=/x-tools/aarch64-unknown-linux-gnu/bin/aarch64-unknown-linux-gnu-c++
        ;;
        darwin_amd64)
            CC_FOR_TARGET=/x-tools/x86_64-apple-darwin21.2/bin/x86_64-apple-darwin21.2-cc
            CXX_FOR_TARGET=/x-tools/x86_64-apple-darwin21.2/bin/x86_64-apple-darwin21.2-c++
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
    GO_GCFLAGS="-I/usr/include" GOOS=$GOOS GOARCH=$GOARCH CC=clang CXX=clang++ CC_FOR_TARGET=$CC_FOR_TARGET CXX_FOR_TARGET=$CXX_FOR_TARGET \
               GOROOT_BOOTSTRAP=$(go env GOROOT) CGO_ENABLED=1 ./make.bash
    if [ $GOOS == darwin ]; then
        unset LD_LIBRARY_PATH
    fi
    cd ../..
    rm -rf /tmp/go$GOVERS/go/pkg/${GOOS}_$GOARCH/cmd
    if [ $CONFIG != linux_amd64 ]; then
        rm go/bin/go go/bin/gofmt
        mv go/bin/${GOOS}_$GOARCH/* go/bin
        rm -r go/bin/${GOOS}_$GOARCH
    fi
    tar cf - go | gzip -9 > /artifacts/go$GOVERS.$GOOS-$GOARCH.tar.gz
    rm -rf go/bin
done

# FIPS patches
cd /tmp/go$GOVERS/go
patch -p1 < /tmp/fips-patches/patches/000-initial-setup.patch
patch -p1 < /tmp/fips-patches/patches/001-initial-openssl-for-fips.patch 
cd ..

for CONFIG in linux_amd64 linux_arm64; do
    case $CONFIG in
        linux_amd64)
            CC_FOR_TARGET=/x-tools/x86_64-unknown-linux-gnu/bin/x86_64-unknown-linux-gnu-cc
            CXX_FOR_TARGET=/x-tools/x86_64-unknown-linux-gnu/bin/x86_64-unknown-linux-gnu-c++
            includes=(/usr/include/openssl /usr/x86_64-linux-gnu/include/bits)
        ;;
        linux_arm64)
            CC_FOR_TARGET=/x-tools/aarch64-unknown-linux-gnu/bin/aarch64-unknown-linux-gnu-cc
            CXX_FOR_TARGET=/x-tools/aarch64-unknown-linux-gnu/bin/aarch64-unknown-linux-gnu-c++
            includes=(/usr/include/openssl /usr/aarch64-linux-gnu/include/bits)
        ;;
    esac
    tmp_includes=$(mktemp -d)
    for i in "${includes[@]}"; do
        ln -s "$i" "$tmp_includes/"
    done
    find "$tmp_includes" -ls
    GOOS=$(echo $CONFIG | cut -d_ -f1)
    GOARCH=$(echo $CONFIG | cut -d_ -f2)
    cd go/src
    CGO_CFLAGS="-I $tmp_includes"
        GOOS=$GOOS GOARCH=$GOARCH CC=clang CXX=clang++ CC_FOR_TARGET=$CC_FOR_TARGET CXX_FOR_TARGET=$CXX_FOR_TARGET \
        GOROOT_BOOTSTRAP=$(go env GOROOT) CGO_ENABLED=1 ./make.bash
    cd ../..
    rm -rf /tmp/go$GOVERS/go/pkg/${GOOS}_$GOARCH/cmd
    if [ $CONFIG != linux_amd64 ]; then
        rm go/bin/go go/bin/gofmt
        mv go/bin/${GOOS}_$GOARCH/* go/bin
        rm -r go/bin/${GOOS}_$GOARCH
    fi
    tar cf - go | gzip -9 > /artifacts/go$GOVERS.$GOOS-$GOARCH-fips.tar.gz
    rm -rf go/bin "$tmp_includes"
done

sha256sum /artifacts/*.tar.gz
