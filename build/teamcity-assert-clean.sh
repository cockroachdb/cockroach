#!/usr/bin/env bash
set -exuo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

build/builder.sh /bin/bash -c '! git status --porcelain | read || (git status; git diff -a 1>&2; exit 1)'
