#!/usr/bin/env bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# This script sets the Bazel runfiles directory env var and calls benchdiff with
# the provided arguments adding the --bazel flag if it is not already present.
# Some test binaries require bazel runfiles to be set in order to execute. This
# script does not support multiple package arguments or package wildcards.

pkg_arg=""
bazel_flag=false

# Loop through all arguments
for arg in "$@"; do
  if [[ "$arg" == "./pkg/"* ]]; then
    pkg_arg="$arg"
  elif [[ "$arg" == "--bazel" ]]; then
    bazel_flag=true
  fi
done

# If --bazel was not found, add it
if [[ "$bazel_flag" == false ]]; then
  set -- "$@" "--bazel"
fi

# Ensure we found the package argument
if [[ -z "$pkg_arg" ]]; then
  echo "Error: Expected a package argument like './pkg/sql' but didn't find one."
  exit 1
fi

pkg_dir="${pkg_arg#./}"
pkg=$(basename "$pkg_arg")
bazel_bin=$(bazel info bazel-bin)

# Set the Bazel runfiles directory
export RUNFILES_DIR="${bazel_bin}/${pkg_dir}/${pkg}_test_/${pkg}_test.runfiles"

benchdiff "$@"
