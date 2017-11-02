#!/usr/bin/env bash
set -exuo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

# The workspace is clean iff `git status --porcelain` produces no output. Any
# output is either an error message or a listing of an untracked/dirty file.
if [[ "$(git status --porcelain 2>&1)" != "" ]]; then
  git status >&2 || true
  git diff -a >&2 || true
  docker run --volume="$(cd "$(dirname "$0")/.." && pwd):/nuke" --workdir="/nuke" golang:stretch git clean -fdx >&2
  exit 1
fi
