#!/bin/bash
set -e
cd "$(dirname $0)/.."

# Verify docker installation.
source "./build/verify-docker.sh"

# We bake the submodule SHAs into a docker layer that is built before
# the submodules are cloned. This ensures that the submodules are
# freshly cloned whenever the SHA pointers are updated.
ETCD_SHA=$(git ls-tree HEAD _vendor/src/github.com/coreos/etcd | awk '{print $3}')
WANTED_CONTENT=$(sed -e "s/ETCD_SHA/${ETCD_SHA}/g" \
    < build/devbase/Dockerfile.template)
if diff <(echo "${WANTED_CONTENT}") "build/devbase/Dockerfile" &> /dev/null; then
  echo "Not updating Dockerfile from template, contents are equal"
else
  echo "Rewriting Dockerfile"
  echo "${WANTED_CONTENT}" > build/devbase/Dockerfile
fi

# Create the docker cockroach image.
echo "Building Docker Cockroach images..."
docker build -t "cockroachdb/cockroach-devbase" ./build/devbase
docker build -t "cockroachdb/cockroach-dev" .
