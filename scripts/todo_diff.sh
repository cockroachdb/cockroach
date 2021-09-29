#!/bin/bash

# The command runs git diff between two refspecs to find all instances of TODO
# added between them.

cmd=$(basename "$0")
__usage="${cmd} finds TODOs in pkg added in a release branch since a release tag.
It prints the results to stdout.

Usage: ${cmd} <release-tag> <release-branch>"

set -euo pipefail

if [ $# -ne 2 ]; then
  echo >&2 "${__usage}"
  exit 1
fi

ORIGINAL_BRANCH=$(git branch --show-current)
TAG=$1
BRANCH=$2
UPSTREAM=$(git remote -v | grep 'cockroachdb/cockroach' | { read -r UPSTREAM rest; echo "${UPSTREAM}"; } )

setup() {

  # Pull latest upstream.
  git fetch --quiet "${UPSTREAM}"

  # Checkout the old release tag.
  git checkout --quiet "tags/${TAG}"
  
  # Checkout the new files for the sql directory.
  git checkout --quiet "${UPSTREAM}/${BRANCH}" pkg/sql
  
  # Reset.
  git reset --quiet HEAD pkg
  
  # Add all untracked files to the index so the will show results with git diff.
  git add -AN pkg
}


cleanup() {
  git restore --quiet --staged pkg
  git restore --quiet pkg
  git clean --quiet -f pkg
  git checkout --quiet --no-recurse-submodules "${ORIGINAL_BRANCH}"
}


diff_file() {
    __file="$1"
    git diff -G'TODO' "${__file}" \
      | { grep '^+.*TODO' || true ; } \
      | sed 's/^\+[[:space:]]*//g' \
      | awk '{printf("%s: %s\n", "'"${__file}"'", $0)}'
}

run() {  
  while read -r -s p || [[ -n "${p}" ]]
  do
    diff_file "${p}"
  done < <( git diff -G'TODO' --name-only pkg )
}

trap cleanup >&2  EXIT
setup >&2
run
