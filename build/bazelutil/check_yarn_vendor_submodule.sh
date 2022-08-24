#!/usr/bin/env bash
set -eu

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 PATH"
  echo "Example: check_yarn_vendor_submodule.sh /path/to/cockroachdb/cockroach/pkg/ui/yarn-vendor"
  exit 1
fi

yarnVendorDir=$1
if [ -z "$(ls -A $yarnVendorDir)" ]; then
  echo "No packages available from yarn-vendor submodule." >&2
  echo "You may need to run 'git submodule update --init'." >&2
  exit 2
fi

