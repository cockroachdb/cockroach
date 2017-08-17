#!/usr/bin/env bash
set -exuo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

if ! build/builder.sh /bin/bash -c '! git status --porcelain | read || (git status; git diff -a 1>&2; exit 1)';
then
  docker run --volume="$(cd "$(dirname "$0")/.." && pwd):/nuke" --workdir="/nuke" golang:stretch git clean -fdx
  exit 1
fi
