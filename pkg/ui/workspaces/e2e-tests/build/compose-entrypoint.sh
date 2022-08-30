#!/usr/bin/env bash
set -uemo pipefail

set -x

# Copy the /e2e tree into /scratch and remove any existing node_modules, to
# prevent files with unexpected owners from escaping
mkdir /scratch
cp --recursive --no-target-directory --no-preserve=owner /e2e /scratch
cd /scratch

# Write a minimal .yarnrc mirroring the one in cockroach.git/pkg/ui, to avoid
# mounting that entire tree.
echo "yarn-offline-mirror /yarn-vendor" > .yarnrc

# Remove and reinstall any node_modules that may have been copied, since they're
# potentially specific to the host platform.
rm -rf node_modules/
yarn install --force --offline

# Run tests, passing extra CLI arguments through to Cypress
yarn cy:run "$@"
