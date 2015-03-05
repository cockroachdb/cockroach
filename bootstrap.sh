#!/bin/bash

# Bootstrap sets up all needed dependencies.
# It's idempotent so if you don't hack often, your best bet is to just run this.
# Assumes you are running from the top of the project.
#
# 1) Update go dependencies
# 2) Update build tools
# 3) Install git hooks

set -e

cd -P "$(dirname $0)"

# go vet is special: it installs into $GOROOT (which $USER may not have
# write access to) instead of $GOPATH. It is usually but not always
# installed along with the rest of the go toolchain. Don't try to
# install it if it's already there.
if ! go vet 2>/dev/null; then
    go get golang.org/x/tools/cmd/vet
fi

# glock is used to manage the rest of our dependencies (and to update
# itself, so no -u here)
go get github.com/robfig/glock

# Grab the go dependencies required for building.
./build/devbase/godeps.sh

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
