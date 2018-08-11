#!/usr/bin/env bash

# This script sanity-checks a source tarball, assuming a Debian-based Linux
# environment with a Go version capable of building CockroachDB. Source tarballs
# are expected to build, even after `make clean`, and install a functional
# cockroach binary into the PATH, even when the tarball is extracted outside of
# GOPATH.

set -euo pipefail

apt-get update
apt-get install -y autoconf bison cmake libncurses-dev

workdir=$(mktemp -d)
tar xzf cockroach.src.tgz -C "$workdir"
(cd "$workdir"/cockroach-* && make clean && make install)

cockroach start-single-node --insecure --store type=mem,size=1GiB --background
cockroach sql --insecure <<EOF
  CREATE DATABASE bank;
  CREATE TABLE bank.accounts (id INT PRIMARY KEY, balance DECIMAL);
  INSERT INTO bank.accounts VALUES (1, 1000.50);
EOF
diff -u - <(cockroach sql --insecure -e 'SELECT * FROM bank.accounts') <<EOF
id	balance
1	1000.50
EOF
cockroach quit --insecure
