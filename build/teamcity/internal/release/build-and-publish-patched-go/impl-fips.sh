#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -xeuo pipefail

GO_FIPS_REPO=https://github.com/golang-fips/go
GO_FIPS_COMMIT=495095e7a12b6ce4cb63f32b9c2677efc298e9e9
GOCOMMIT=$(grep -v ^# /bootstrap/commit.txt | head -n1)

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
GOLANG_REPO=https://github.com/cockroachdb/go.git ./scripts/full-initialize-repo.sh "$GOCOMMIT"
cd go/src
# add a special version modifier so we can explicitly use it in bazel
sed -i '1 s/$/fips/' ../VERSION
./make.bash -v
cd ../..
GOVERS=$(go/bin/go env GOVERSION)
GOOS=$(go/bin/go env GOOS)
GOARCH=$(go/bin/go env GOARCH)
tar cf - go | gzip -9 > /artifacts/$GOVERS.$GOOS-$GOARCH.tar.gz

sha256sum /artifacts/$GOVERS.$GOOS-$GOARCH.tar.gz
