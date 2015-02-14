#!/bin/bash

# Bootstrap sets up all needed dependencies.
# It's idempotent so if you don't hack often, your best bet is to just run this.
# Assumes you are running from the top of the project.
#
# 1) Update all source code and submodules
# 2) Update go dependencies
# 3) Build a shadow toolchain containing our dependencies in _vendor/build

# TODO(shawn) make rocksdb build less magic
# TODO(shawn) make sure rocksdb still links against jemalloc (and that it makes sense when embedding in go)
# TODO(pmattis): check for pkg-config and curl.

cd -P "$(dirname $0)"

set -e -x

# Update submodules
git submodule update --init

function go_get() {
  go get -u -v "$@"
}

# Grab binaries required by git hooks.
go_get github.com/golang/lint/golint
go_get golang.org/x/tools/cmd/goimports
# go vet is special: it installs into $GOROOT (which $USER may not have
# write access to) instead of $GOPATH. It is usually but not always
# installed along with the rest of the go toolchain. Don't try to
# install it if it's already there.
go vet -n 2>/dev/null || go_get golang.org/x/tools/cmd/vet

# Grab the go dependencies required for building.
./build/devbase/godeps.sh

# Create symlinks to all git hooks in your own .git dir.
for f in $(ls -d githooks/*); do
  rm .git/hooks/$(basename $f)
  ln -s ../../$f .git/hooks/$(basename $f)
done && ls -al .git/hooks | grep githooks

# Build the required libraries.
./build/devbase/vendor.sh

cat <<%%%
****************************************
Bootstrapped successfully! You don't need to do anything else.
****************************************
%%%
