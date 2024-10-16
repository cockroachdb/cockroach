#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -xeuo pipefail

GO_FIPS_REPO=https://github.com/golang-fips/go
GO_FIPS_COMMIT=8092b8157908b59e5930a1247b6f41842a25f89e
GO_VERSION=1.22.5

# Install build dependencies
yum install git golang golang-bin openssl openssl-devel -y
cat /etc/os-release
go version
openssl version -a
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
patch -p2 </bootstrap/diff.patch
# add a special version modifier so we can explicitly use it in bazel
sed -i '1 s/$/fips/' ../VERSION
./make.bash -v
cd ../..
GOVERS=$(go/bin/go env GOVERSION)
GOOS=$(go/bin/go env GOOS)
GOARCH=$(go/bin/go env GOARCH)
tar cf - go | gzip -9 > /artifacts/$GOVERS.$GOOS-$GOARCH.tar.gz

sha256sum /artifacts/$GOVERS.$GOOS-$GOARCH.tar.gz
