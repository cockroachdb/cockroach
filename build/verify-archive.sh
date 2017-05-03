#!/usr/bin/env bash

# This script sanity-checks a source tarball, assuming a Debian-based Linux
# environment with a Go version capable of building CockroachDB. Source tarballs
# are expected to build and install a functional cockroach binary into the PATH,
# even when the tarball is extracted outside of GOPATH.

set -euo pipefail

apt-get update
apt-get install -y cmake xz-utils

workdir=$(mktemp -d)
tar xzf cockroach.src.tgz -C "$workdir"
(cd "$workdir"/cockroach-* && make install)

cockroach start --insecure --store type=mem,size=1GiB --background
cockroach sql --insecure <<EOF | diff <(echo $'1 row\nid	balance\n1	1000.50') -
  CREATE DATABASE bank; \
  CREATE TABLE bank.accounts (id INT PRIMARY KEY, balance DECIMAL); \
  INSERT INTO bank.accounts VALUES (1, 1000.50); \
  SELECT * FROM bank.accounts;" \
EOF
cockroach quit --insecure
