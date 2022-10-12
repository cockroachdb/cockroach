#!/bin/bash

# Script to perform code generation. This exists to overcome
# the fact that go:generate doesn't really allow you to change directories

set -e

pushd internal/cmd/genreadfile
go build -o genreadfile main.go
popd

./internal/cmd/genreadfile/genreadfile 

rm internal/cmd/genreadfile/genreadfile
