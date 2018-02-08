#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

# The workspace is clean iff `git status --porcelain` produces no output. Any
# output is either an error message or a listing of an untracked/dirty file.
if [[ "$(git status --porcelain 2>&1)" != "" ]]; then
  git status >&2 || true
  git diff -a >&2 || true
  echo "Nuking build cruft. Please teach this build to clean up after itself." >&2
  run docker run --volume="$root:/nuke" --workdir="/nuke" golang:stretch git clean -fdx >&2
  exit 1
fi
