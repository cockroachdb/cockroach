#!/usr/bin/env bash

# Verify whether the CockroachDB binary installed on the system is capable of
# executing some basic SQL queries with an on-disk store.

set -euo pipefail

cockroach start --insecure --background
cockroach sql --insecure <<EOF
  CREATE DATABASE bank;
  CREATE TABLE bank.accounts (id INT PRIMARY KEY, balance DECIMAL);
  INSERT INTO bank.accounts VALUES (1, 1000.50);
EOF
diff -u - <(cockroach sql --insecure -e 'SELECT * FROM bank.accounts') <<EOF
id	balance
1	1000.50
# 1 row
EOF
cockroach quit --insecure
