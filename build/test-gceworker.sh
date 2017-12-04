#!/usr/bin/env bash

# Test whether a GCE worker is capable of building from a fresh clone.
#
# GCE workers are an escape hatch for developers if their build breaks locally,
# so it's important that the build isn't broken on GCE workers too. Before this
# test existed, it was all too easy to add a new compile-time dependency without
# adding it to the GCE worker bootstrap script or to refactor the Makefile in a
# way that broke fresh clones but not existing clones.

cd "$(dirname "$0")/.."

trap 'scripts/gceworker.sh delete --quiet' EXIT
scripts/gceworker.sh create
scripts/gceworker.sh ssh <<'EOF'
  set -euxo pipefail
  cd "$(go env GOPATH)/src/github.com/cockroachdb/cockroach"
  make build
  sudo make install
EOF
scripts/gceworker.sh ssh < build/smoke-test.sh
