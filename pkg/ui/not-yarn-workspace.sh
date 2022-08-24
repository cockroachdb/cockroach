#!/usr/bin/env bash
set -euo pipefail

(
  set -x
  yarn --cwd workspaces/eslint-plugin-crdb install
  yarn --cwd workspaces/db-console/src/js install
  yarn --cwd workspaces/cluster-ui install
  yarn --cwd workspaces/db-console install
  yarn --cwd workspaces/e2e-tests install
)

cat << "EOF"
=================================== WARNING ====================================
As-of May 2022, the pkg/ui/ tree is no longer a "yarn workspace", due to Bazel's
lack of support for them. When adding, updating, or otherwise modifying JS
dependencies, be sure to `cd` into the correct package within workspaces/.

As a convenience, running `yarn install` from pkg/ui/ still installs the
dependencies for all packages within workspaces/.
================================================================================
EOF
