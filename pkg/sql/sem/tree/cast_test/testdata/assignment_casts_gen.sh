#!/usr/bin/env bash
set -euo pipefail

# assignment_casts_gen.sh generates a CSV file of test cases for use by
# TestAssignmentCastsMatchPostgres, based on the files 'literals.txt' and
# 'types.txt'. To use this script, Postgres must be installed locally with the
# PostGIS extension and must already be running.
#
# Usage:
#   ./assignment_casts_gen.sh > assignment_casts.csv

echo 'literal,type,expect'
while read -r type; do
  psql -qc "CREATE TABLE assignment_casts (val $type)"
  while read -r literal; do
    # Quote literal and type in case they contain quotes or commas.
    printf '"%s","%s",' "${literal//\"/\"\"}" "${type//\"/\"\"}"
    psql --csv -qtc "INSERT INTO assignment_casts VALUES ($literal) RETURNING quote_nullable(val)" 2>/dev/null || echo 'error'
  done <literals.txt
  psql -qc 'DROP TABLE IF EXISTS assignment_casts' 2>/dev/null
done <types.txt
