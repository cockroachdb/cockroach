#!/usr/bin/env bash
set -exuo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

mkdir -p artifacts

build/builder.sh make check 2>&1 | tee artifacts/check.log | go-test-teamcity

build/builder.sh make generate
build/builder.sh /bin/bash -c '! git status --porcelain | read || (git status; git diff -a 1>&2; exit 1)'

build/builder.sh make -C pkg/ui test
