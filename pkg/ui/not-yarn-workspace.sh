#!/usr/bin/env bash
set -euo pipefail

(
  set -x
  yarn --cwd workspaces/eslint-plugin-crdb --pure-lockfile install
  yarn --cwd workspaces/db-console/src/js --pure-lockfile install
  yarn --cwd workspaces/cluster-ui --pure-lockfile install
  yarn --cwd workspaces/db-console --pure-lockfile install
  # Don't install the native Cypress binary immediately. ./dev ui e2e will do that just in time.
  CYPRESS_INSTALL_BINARY=0 yarn --cwd workspaces/e2e-tests --pure-lockfile install
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
