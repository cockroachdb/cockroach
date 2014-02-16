#!/bin/bash

set -e -x

# Grab binaries required by git hooks.
go get github.com/golang/lint/golint
go get code.google.com/p/go.tools/cmd/goimports

# Create symlinks to all git hooks in your own .git dir.
for f in $(ls -d githooks/*)
do ln -s ../../$f .git/hooks
done && ls -al .git/hooks | grep githooks
