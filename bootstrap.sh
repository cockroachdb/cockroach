#!/bin/bash

GO_GET="go get"
GO_GET_FLAGS="-u -v"

set -e -x

# Init submodules.
git submodule init
git submodule update

# Grab binaries required by git hooks.
$GO_GET $GO_GET_FLAGS github.com/golang/lint/golint
$GO_GET $GO_GET_FLAGS code.google.com/p/go.tools/cmd/vet
$GO_GET $GO_GET_FLAGS code.google.com/p/go.tools/cmd/goimports

# Grab dependencies.
$GO_GET $GO_GET_FLAGS code.google.com/p/biogo.store/llrb
$GO_GET $GO_GET_FLAGS code.google.com/p/go-commander
$GO_GET $GO_GET_FLAGS code.google.com/p/go-uuid/uuid
$GO_GET $GO_GET_FLAGS code.google.com/p/gogoprotobuf/{proto,protoc-gen-gogo,gogoproto}
$GO_GET $GO_GET_FLAGS github.com/golang/glog
$GO_GET $GO_GET_FLAGS gopkg.in/yaml.v1

# Create symlinks to all git hooks in your own .git dir.
for f in $(ls -d githooks/*); do
  rm .git/hooks/$(basename $f)
  ln -s ../../$f .git/hooks/$(basename $f)
done && ls -al .git/hooks | grep githooks
