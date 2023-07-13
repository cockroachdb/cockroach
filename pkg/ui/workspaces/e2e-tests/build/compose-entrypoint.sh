#!/usr/bin/env bash
set -uemo pipefail

set -x

# Copy the /e2e tree into /scratch and remove any existing node_modules, to
# prevent files with unexpected owners from escaping
mkdir /scratch
cp --recursive --no-target-directory --no-preserve=owner /e2e /scratch
cd /scratch

# Remove and reinstall any node_modules that may have been copied, since they're
# potentially specific to the host platform.
rm -rf node_modules/
pnpm install --filter . --force --frozen-lockfile

# Run tests, passing extra CLI arguments through to Cypress
pnpm run cy:run "$@"
