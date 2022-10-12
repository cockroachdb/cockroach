#!/bin/bash

# Script to perform code generation. This exists to overcome
# the fact that go:generate doesn't really allow you to change directories

set -e

pushd internal/cmd/gentypes
go build -o gentypes main.go
popd

./internal/cmd/gentypes/gentypes -objects=internal/cmd/gentypes/objects.yml

rm internal/cmd/gentypes/gentypes
