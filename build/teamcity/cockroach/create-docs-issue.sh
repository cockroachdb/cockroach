#!/usr/bin/env bash

set -exuo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

build/builder.sh go build -o bin/docs-issue-generation ./pkg/cmd/docs-issue-generation
./bin.docker_amd64/docs-issue-generation
