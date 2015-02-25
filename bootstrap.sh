#!/bin/bash

# Bootstrap sets up all needed dependencies.
# It's idempotent so if you don't hack often, your best bet is to just run this.
# Assumes you are running from the top of the project.
#
# 1) Update all source code and submodules
# 2) Update go dependencies
# 3) Build a shadow toolchain containing our dependencies in _vendor/build

cd -P "$(dirname $0)"

PKGS="github.com/golang/lint/golint"
PKGS="${PKGS} golang.org/x/tools/cmd/goimports"

# go vet is special: it installs into $GOROOT (which $USER may not have
# write access to) instead of $GOPATH. It is usually but not always
# installed along with the rest of the go toolchain. Don't try to
# install it if it's already there.
if ! go vet 2>/dev/null; then
    PKGS="${PKGS} golang.org/x/tools/cmd/vet"
fi

set -ex

# Update submodules
git submodule update --init

# Grab binaries required by git hooks.
go get -u ${PKGS}

# Grab the go dependencies required for building.
./build/devbase/godeps.sh

go install -v \
   github.com/cockroachdb/c-protobuf/cmd/protoc \
   github.com/gogo/protobuf/protoc-gen-gogo

set +x

# Create symlinks to all git hooks in your own .git dir.
for f in $(ls -d githooks/*); do
  rm .git/hooks/$(basename $f)
  ln -s ../../$f .git/hooks/$(basename $f)
done && ls -al .git/hooks | grep githooks

cat <<%%%
****************************************
Bootstrapped successfully! You don't need to do anything else.
****************************************
%%%
