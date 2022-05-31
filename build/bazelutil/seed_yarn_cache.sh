#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 PKG_PATH MODULES_DIR"
  echo "Example: seed_yarn_cache.sh /path/to/cockroachdb/cockroach/pkg/ui/workspaces/cluster-ui /path/to/repo/node_modules.cluster_ui"
  exit 1
fi

rootDir=$1
moduleDir=$2

yarn \
  --cwd $rootDir \
  --modules-folder $moduleDir \
  --offline \
  --ignore-optional \
  --no-progress \
  --mutex network \
  install
