#!/usr/bin/env bash

set -xeuo pipefail

# TODO: create a fork?
GO_FIPS_REPO=https://github.com/golang-fips/go
GO_FIPS_BRANCH=go1.19-fips-release


yum install git golang golang-bin openssl openssl-devel -y
cat /etc/os-release
go version
openssl version
git config --global user.name "golang-fips ci"
git config --global user.email "<>"

mkdir /workspace
cd /workspace
git clone $GO_FIPS_REPO go
cd go
git init
git config --global --add safe.directory /__w/go/go
git checkout $GO_FIPS_BRANCH
sed -i "s/go mod tidy/go mod tidy -go=1.16/g" scripts/create-secondary-patch.sh
./scripts/full-initialize-repo.sh
./scripts/configure-crypto-tests.sh
cd go/src
patch -p2 < /bootstrap/diff.patch
./make.bash -v
cd ../..
GOVERS=$(go/bin/go env GOVERSION)
GOOS=$(go/bin/go env GOOS)
GOARCH=$(go/bin/go env GOARCH)
tar cf - go | gzip -9 > /artifacts/$GOVERS.$GOOS-$GOARCH.tar.gz
