#!/bin/bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euo pipefail

function usage() {
  echo "This script runs gazelle and applies a patch to a repository clone."
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

# Check if the patch file exists
if [ ! -f "$PATCH_FILE" ]; then
  echo "Error: Patch file '$PATCH_FILE' does not exist."
  exit 1
fi

# Check if the repository directory exists
if [ ! -d "$REPO_DIR" ]; then
  echo "Error: Repository directory '$REPO_DIR' does not exist."
  exit 1
fi

cd "$REPO_DIR" || exit 1

# Apply the patch
echo "Applying patch $PATCH_FILE to directory $REPO_DIR"
echo ""

if patch -p1 < "$PATCH_FILE"; then
  echo "Patch applied successfully."
else
  echo "Failed to apply patch."
  echo "Make sure the repository is at the proper commit."
  exit 1
fi
