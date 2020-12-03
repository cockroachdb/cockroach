#!/bin/bash

set -e

declare -a VERSIONS=( v19.1.11 v19.2.11 v20.1.8 v20.2.1 )

declare -A VERSION_STATEMENTS=(
  [v19.1.11]="
CREATE DATABASE db1;
USE db1;
CREATE TABLE a (i INT PRIMARY KEY, j INT UNIQUE);
CREATE TABLE b (i INT PRIMARY KEY, j INT UNIQUE REFERENCES a (j));
-- c.j will get an auto-created index.
CREATE TABLE c (i INT PRIMARY KEY, j INT REFERENCES b (j));

CREATE DATABASE db2;
USE db2;
CREATE TABLE a (i INT PRIMARY KEY, j INT UNIQUE, INDEX(j, i));
CREATE TABLE b (i INT PRIMARY KEY, j INT UNIQUE REFERENCES a (j));
-- c.j will get an auto-created index.
CREATE TABLE c (i INT PRIMARY KEY, j INT REFERENCES b (j), k INT, INDEX idx_k (k));
-- Create some extra indexes.
CREATE INDEX extra_idx_1 ON c(j);
CREATE INDEX extra_idx_2 ON c(j, k);

CREATE DATABASE db3;
USE db3;
CREATE TABLE a (i INT PRIMARY KEY, j INT);
CREATE UNIQUE INDEX a_idx_j ON a(j);
CREATE TABLE b (i INT PRIMARY KEY, j INT, k INT);
CREATE INDEX b_idx_j ON b(j);
ALTER TABLE b ADD FOREIGN KEY (j) REFERENCES a(j);
CREATE TABLE c (i INT PRIMARY KEY, j INT, k INT);
CREATE INDEX c_idx_j ON c(j);
ALTER TABLE c ADD FOREIGN KEY (j) REFERENCES a(j);

CREATE DATABASE db4;
USE db4;
CREATE TABLE a (i INT PRIMARY KEY, j INT);
CREATE TABLE b (i INT PRIMARY KEY, j INT);
ALTER TABLE a ADD COLUMN k INT;
ALTER TABLE a DROP COLUMN k;
ALTER TABLE a DROP COLUMN j;
ALTER TABLE a ADD COLUMN j INT;
ALTER TABLE a ADD COLUMN k INT UNIQUE;

ALTER TABLE b ADD COLUMN k INT UNIQUE;

ALTER TABLE a ADD FOREIGN KEY (k) REFERENCES b(k);
"
  [v19.2.11]="
-- These schema changes will upgrade the FKs on the respective descriptors.
-- Keep db1.b unupgraded, but upgrade the other tables in its reference graph.
ALTER TABLE db1.a RENAME COLUMN j TO k;
ALTER TABLE db1.c RENAME COLUMN j TO k;

-- Unrelated DROP INDEX.
DROP INDEX db2.c@idx_k;
"
  [v20.1.8]="
-- Swap out a referencing-side index. This should upgrade db3.a, but we leave db3.c alone.
USE db3;
CREATE INDEX b_idx_j_new ON b(j, k);
DROP INDEX b_idx_j;
"
  [v20.2.1]=''
)


main() {
  check_local_does_not_exist
  install_cockroach_versions
  run_version_statements
  dump_descriptors
}

check_local_does_not_exist() {
  out="$( { roachprod run local true 2>&1 || true ; } | head )"
  if [[ "${out}" =~ "unknown cluster: local" ]]; then
    return 0
  fi
  echo >&2 "make sure the local cluster does not exist"
  exit 1
}

install_cockroach_versions() {
  roachprod create local -n 1
  for v in "${VERSIONS[@]}"; do
    roachprod stage local release "${v}"
    roachprod run local cp ./cockroach "./cockroach-${v}"
  done
}

run_version_statements() {
  for v in "${VERSIONS[@]}"; do
    roachprod stop local
    roachprod start local --binary cockroach-$v
    sleep 1 # wait for the upgrade to happen
    stmts="${VERSION_STATEMENTS[$v]}"
    if [[ -z "${stmts}" ]]; then
      continue
    fi
    roachprod sql local -- -e "${stmts}"
  done
}

dump_descriptors() {
  roachprod sql local -- \
            --format csv \
            -e "SELECT id, encode(descriptor, 'hex') AS descriptor FROM system.descriptor" \
            > descriptors.csv 
}

main
