#!/usr/bin/env bash
set -euo pipefail

(
  set -x
  yarn --cwd packages/db-console/src/js install
  yarn --cwd packages/cluster-ui install
  yarn --cwd packages/db-console install
)

cat << "EOF"
=================================== WARNING ====================================
As-of May 2022, the pkg/ui/ tree is no longer a "yarn workspace", due to Bazel's
lack of support for them. When adding, updating, or otherwise modifying JS
dependencies, be sure to `cd` into the correct package within packages/.

As a convenience, running `yarn install` from pkg/ui/ still installs the
dependencies for all packages within packages/.
================================================================================
EOF
