#!/usr/bin/env bash

# This script sanity-checks a source tarball, assuming a Debian-based Linux
# environment with a Go version capable of building CockroachDB. Source tarballs
# are expected to build, even after `make clean`, and install a functional
# cockroach binary into the PATH, even when the tarball is extracted outside of
# GOPATH.

set -euo pipefail

apt-get update
apt-get install -y autoconf cmake libtinfo-dev

workdir=$(mktemp -d)
tar xzf cockroach.src.tgz -C "$workdir"
(cd "$workdir"/cockroach-* && make clean && make install)

./smoke-test.sh
