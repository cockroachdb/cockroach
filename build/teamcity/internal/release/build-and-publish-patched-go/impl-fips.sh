#!/usr/bin/env bash

set -xeuo pipefail

# TODO: We may want to fork this repo and keep it up to date.
GO_FIPS_REPO=https://github.com/golang-fips/go
GO_FIPS_COMMIT=go1.19-fips-release
GO_VERSION=1.19.8

# Install build dependencies
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
git checkout $GO_FIPS_COMMIT
# Lower the requirements in case we need to bootstrap with an older Go version
sed -i "s/go mod tidy/go mod tidy -go=1.16/g" scripts/create-secondary-patch.sh
./scripts/full-initialize-repo.sh "go$GO_VERSION"
cd go/src
# Apply the CRL patch
patch -p2 < /bootstrap/diff.patch
# add a special version modifier so we can explicitly use it in bazel
sed -i 's/$/fips/' ../VERSION 
./make.bash -v
cd ../..
GOVERS=$(go/bin/go env GOVERSION)
GOOS=$(go/bin/go env GOOS)
GOARCH=$(go/bin/go env GOARCH)
tar cf - go | gzip -9 > /artifacts/$GOVERS.$GOOS-$GOARCH.tar.gz
