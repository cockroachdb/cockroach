#!/bin/bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euo pipefail

function usage() {
  echo "This script will create a patch file from the current git diff from a"
  echo "repository clone."
  echo ""
  echo "Assumes that the repository has been prepared using patch-prepare-repo.sh"
  echo ""
  echo "Usage: $0 <bazel-repo-name> <repo-directory>"
  echo ""
  echo "Example: $0 com_github_cockroachdb_pebble ~/pebble"
  exit 1
}

if [ "$#" -ne 2 ]; then
  usage
fi

CRDB_ROOT="$(dirname $(dirname $(realpath $0)))"

REPO_NAME="$1"
REPO_DIR="$2"

PATCH_FILE="$CRDB_ROOT/patches/$REPO_NAME.patch"

# Check if the repository directory exists
if [ ! -d "$REPO_DIR" ]; then
  echo "Error: Repository directory '$REPO_DIR' does not exist."
  exit 1
fi

# Check if the directory is a git repository
if [ ! -d "$REPO_DIR/.git" ]; then
  echo "Error: '$REPO_DIR' is not a Git repository."
  exit 1
fi

echo "Creating $REPO_NAME.patch from the current git diff in $REPO_DIR"
echo ""
echo "Note: This diff contains staged changes and unstaged changes to tracked"
echo "      files. It does not include untracked files."
echo ""

cd "$REPO_DIR" || exit 1
if git diff --no-ext-diff HEAD > "$PATCH_FILE"; then
  echo "Patch file created successfully at $PATCH_FILE"

  echo ""
  echo "Note that the gazelle invocation does not use all flags that are normally"
  echo "passed in the bazel build, so the generated files might be different."
  echo "If the generated patch fails to apply, use:"
  echo "  bazel query @<bazel-repo-name>//:<library_name> --output=build"
  echo "without any patch to see the correct \"base\" BUILD.bazel file."
else
  echo "Failed to create patch file."
  exit 1
fi

