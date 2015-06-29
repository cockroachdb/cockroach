#!/bin/bash

set -eu

cd "$(dirname $0)/.."

# Verify docker installation.
source ./build/init-docker.sh

cp -p GLOCKFILE build/devbase
mkdir -p build/devbase/ui
cp -p ui/package.json build/devbase/ui
cp -p ui/npm-shrinkwrap.json build/devbase/ui
# Creating this here helps to not break the cache during deployment runs.
mkdir -p build/deploy/build

# Create the docker cockroach image.
echo "Building Docker Cockroach images..."
docker build -t "cockroachdb/cockroach-devbase" ./build/devbase
docker build -t "cockroachdb/cockroach-dev" .
