#!/bin/bash
set -e
cd "$(dirname $0)/.."

# Verify docker installation.
source "./build/verify-docker.sh"

# We bake the submodule SHAs into a docker layer that is built before
# the submodules are cloned. This ensures that the submodules are
# freshly cloned whenever the SHA pointers are updated.
ROCKSDB_SHA=$(git ls-tree HEAD _vendor/rocksdb | awk '{print $3}')
ETCD_SHA=$(git ls-tree HEAD _vendor/src/github.com/coreos/etcd | awk '{print $3}')
sed -e "s/ROCKSDB_SHA/${ROCKSDB_SHA}/g" \
    -e "s/ETCD_SHA/${ETCD_SHA}/g" \
    < build/devbase/Dockerfile.template > build/devbase/Dockerfile

# Create the docker cockroach image.
echo "Building Docker Cockroach images..."
docker build -t "cockroachdb/cockroach-devbase" ./build/devbase
docker build -t "cockroachdb/cockroach-dev" .
