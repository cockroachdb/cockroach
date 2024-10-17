#!/bin/bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


# The command runs git diff between two refspecs to find all instances of TODO
# added between them.

cmd=$(basename "$0")
__usage="${cmd} finds TODOs in pkg added in a release branch since a release tag.
It prints the results to stdout. The optional third argument represents the
directory within the repo to search.

Usage: ${cmd} <release-tag> <release-branch> [<diff-path>]"

set -euo pipefail

SEARCH_DIR="pkg"

case $# in
  2) ;;
  3) SEARCH_DIR="$3" ;;
  *)
    echo >&2 "${__usage}"
    exit 1
    ;;
esac

ORIGINAL_BRANCH=$(git branch --show-current)
TAG=$1
BRANCH=$2
UPSTREAM=$(git remote -v | grep 'cockroachdb/cockroach' | { read -r UPSTREAM rest; echo "${UPSTREAM}"; } )

setup() {

  # Pull latest upstream.
  git fetch --quiet "${UPSTREAM}"

  # Checkout the old release tag.
  git checkout --quiet "tags/${TAG}"

  # Checkout the new files for the search directory.
  git checkout --quiet "${UPSTREAM}/${BRANCH}" "${SEARCH_DIR}"

  # Reset.
  git reset --quiet HEAD "${SEARCH_DIR}"

  # Add all untracked files to the index so they will show results with git diff.
  git add -AN "${SEARCH_DIR}"
}

cleanup() {
  git restore --quiet --staged "${SEARCH_DIR}"
  git restore --quiet "${SEARCH_DIR}"
  git clean --quiet -f "${SEARCH_DIR}"
  git checkout --quiet --no-recurse-submodules "${ORIGINAL_BRANCH}"
}

diff_file() {
    __file="$1"
    git diff -G'TODO' "${__file}" \
      | {
      # Use a subshell because grep returns a non-zero exit code with no matches.
      # The pattern will match a leading + and then anything such that there is
      # a subsequent TODO and the character preceding it is either a whitespace,
      # #, or /.
      grep -E '^\+(.*[[:space:]#/])?TODO' || true
    } \
      | sed 's|^\+[[:space:]]*\(.*\)|'"${__file}"': \1|g'
}

run() {
  while read -r -s p || [[ -n "${p}" ]]
  do
    diff_file "${p}"
  done < <( git diff -G'TODO' --name-only "${SEARCH_DIR}" )
}

trap cleanup >&2 EXIT
setup >&2
run
