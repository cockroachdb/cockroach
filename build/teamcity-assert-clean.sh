#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"

tmp_artifacts="$(mktemp -d -t artifacts-XXXXXXXXXX --tmpdir="$(dirname $root)")"

cleanup_on_completion() {
  remove_files_on_exit

  # Restore the artifacts to the root directory so TeamCity can pick them up
  if [ -d "${tmp_artifacts}/artifacts" ] ; then
    mv "${tmp_artifacts}/artifacts" "$root"
  fi

  rm -rf "$tmp_artifacts"
}
trap cleanup_on_completion EXIT

# The workspace is clean iff `git status --porcelain` produces no output. Any
# output is either an error message or a listing of an untracked/dirty file.
if [[ "$(git status --porcelain 2>&1)" != "" ]]; then
  # Move the artifacts away so they are not nuked
  if [ -d "${root}/artifacts" ] ; then
    mv "${root}/artifacts" "$tmp_artifacts"
  fi

  git status >&2 || true
  git diff -a >&2 || true
  echo "Nuking build cruft. Please teach this build to clean up after itself." >&2
  run docker run --volume="$root:/nuke" --workdir="/nuke" golang:stretch /bin/bash -c "git clean -ffdx ; git submodule foreach --recursive git clean -xffd" >&2
  exit 1
fi
