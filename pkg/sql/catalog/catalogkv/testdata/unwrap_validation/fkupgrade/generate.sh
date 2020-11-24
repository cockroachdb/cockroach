#!/bin/bash

set -e

declare -a VERSIONS=( v19.1.11 v19.2.11 v20.1.8 v20.2.1 )

declare -A VERSION_STATEMENTS=(
  [v19.1.11]="
CREATE DATABASE db1;
USE db1;
CREATE TABLE a (i INT PRIMARY KEY, j INT UNIQUE);
CREATE TABLE b (i INT PRIMARY KEY, j INT UNIQUE REFERENCES a (j));
CREATE TABLE c (i INT PRIMARY KEY, j INT UNIQUE REFERENCES b (j));

CREATE DATABASE db2;
USE db2;
CREATE TABLE a (i INT PRIMARY KEY, j INT UNIQUE, INDEX(j, i));
CREATE TABLE b (i INT PRIMARY KEY, j INT UNIQUE REFERENCES a (j));
CREATE TABLE c (i INT PRIMARY KEY, j INT UNIQUE REFERENCES b (j), k INT, INDEX idx_k (k));
"
  [v19.2.11]="
DROP INDEX db2.c@idx_k;
ALTER TABLE db1.c RENAME COLUMN j TO k;
"
  [v20.1.8]=''
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
