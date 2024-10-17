#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


# This script is the final step of the "Publish Coverage" build.
#
# It moves the HTML mini-websites into archives, so that it's easy to download
# them individually from the artifacts.

set -euo pipefail

for dir in $(find output/html -mindepth 1 -maxdepth 1 -type d); do
  name=$(basename "$dir")
  echo "Archiving $name.."
  pushd "$dir" > /dev/null
  tar czf "../$name.tar.gz" *
  popd > /dev/null
  rm -rf "$dir"
done
