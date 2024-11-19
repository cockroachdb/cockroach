#!/usr/bin/env bash

# Copyright 2016 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail

function sql() {
  # TODO(knz): Why does the more idiomatic read from stdin not produce any
  # output?
  kubectl exec "cockroachdb-${1}" -- /cockroach/cockroach sql \
      --host "cockroachdb-${1}.cockroachdb" \
      -e "$(cat /dev/stdin)"
}

function kill() {
  ! kubectl exec -t "cockroachdb-${1}" -- /bin/bash -c "while true; do kill 1; done" &> /dev/null
}

# Create database on second node (idempotently for convenience).
cat <<EOF | sql 1
CREATE DATABASE IF NOT EXISTS foo;
CREATE TABLE IF NOT EXISTS foo.bar (k STRING PRIMARY KEY, v STRING); 
UPSERT INTO foo.bar VALUES ('Kuber', 'netes'), ('Cockroach', 'DB');
EOF

# Kill the node we just created the table on.
kill 1

# Read the data from all other nodes (we could also read from the one we just
# killed, but it's awkward to wait for it to respawn).
for i in 0 2 3 4; do
  cat <<EOF | sql "${i}"
SELECT CONCAT(k, v) FROM foo.bar;
EOF
done
