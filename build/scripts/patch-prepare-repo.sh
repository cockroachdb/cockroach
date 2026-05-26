#!/bin/bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euo pipefail

function usage() {
  echo "This script prepares a clone of a dependency for applying and generating"
  echo "patches. It generates a WORKSPACE file and runs gazelle to generate"
  echo "BUILD.bazel files and commits these changes."
  echo ""
  echo "This script should be used to set up the \"base\" tree before using"
  echo "patch-apply.sh and patch-gen.sh."
  echo ""
  echo "Usage: $0 <bazel-repo-name> <repo-directory> [flags for gazelle]"
  echo ""
  echo "Example: $0 com_github_cockroachdb_pebble ~/pebble"
  exit 1
}

# Check for required arguments
if [ "$#" -lt 2 ]; then
  usage
fi

CRDB_ROOT="$(dirname $(dirname $(realpath $0)))"

REPO_NAME="$1"
REPO_DIR="$2"

# Generate the import path from the workspace. For example,
# com_github_cockroachdb_pebble.patch to github.com/cockroachdb/pebble.

# Split the input string by underscores
IFS='_' read -r -a fields <<< "$REPO_NAME"

# Extract the first two fields
field1="${fields[0]}"
field2="${fields[1]}"

# Join the remaining fields with '/'
rest=$(IFS=/; echo "${fields[*]:2}")

IMPORT_PATH="${field2}.${field1}/${rest}"

echo "Import path: $IMPORT_PATH"

# Check if the repository directory exists.
if [ ! -d "$REPO_DIR" ]; then
  echo "Error: Repository directory '$REPO_DIR' does not exist."
  exit 1
fi

cd "$REPO_DIR"

if grep -q -E '^[::space::]*(/WORKSPACE|BUILD.bazel)[::space::]*$' .gitignore; then
  echo "Removing /WORKSPACE and BUILD.bazel from .gitignore"

  TEMP_FILE=$(mktemp)
  cp .gitignore $TEMP_FILE
  sed -E '\#^[::space::]*(/WORKSPACE|BUILD.bazel)[::space::]*$#d' $TEMP_FILE >.gitignore

  git commit .gitignore -m "gitignore: /WORKSPACE and BUILD.bazel"
  git log -1 HEAD
fi

if [ ! -f WORKSPACE ]; then
	echo "Generating WORKSPACE"
	echo "workspace(name = \"$REPO_NAME\")" > WORKSPACE
fi

(
  cd "$CRDB_ROOT"
  set -x
  bazel run @bazel_gazelle//cmd/gazelle -- update ${@:3} --go_naming_convention=import_alias --go_prefix="$IMPORT_PATH" --repo_root=$REPO_DIR $REPO_DIR
)

git add -A
git commit -m "gazelle: generate WORKSPACE and BUILD.bazel"
git log -1 HEAD

