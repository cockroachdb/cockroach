#!/bin/bash

# Script to perform code generation. This exists to overcome
# the fact that go:generate doesn't really allow you to change directories

set -e

pushd internal/cmd/genheader
go build -o genheader main.go
popd

./internal/cmd/genheader/genheader -objects=internal/cmd/genheader/objects.yml

rm internal/cmd/genheader/genheader
