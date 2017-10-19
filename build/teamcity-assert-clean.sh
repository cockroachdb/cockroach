#!/usr/bin/env bash
set -exuo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

# Delete all root-owned files (workaround for lack of sudo on teamcity agents).
if find .. -user root -print -exec docker run --volume="$(cd "$(dirname "$0")/.." && pwd):/nuke" --workdir="/nuke" golang:stretch rm -rf {} + | grep . 1>&2; then
  echo "Found root-owned files! Listed above." 1>&2
  exit 1
fi

# Check for accidentally modified or created non-ignored files.
if ! build/builder.sh /bin/bash -c '! git status --porcelain | read || (git status; git diff -a 1>&2; exit 1)'; then
  echo "Found extra or modified files! Listed above." 1>&2
  exit 1
fi
