#!/usr/bin/env bash
set -euo pipefail

# explicit_casts_gen.sh generates a CSV file of test cases for use by
# TestExplicitCastsMatchPostgres, based on the files 'literals.txt' and
# 'types.txt'. To use this script, Postgres must be installed locally with the
# PostGIS extension and must already be running.
#
# Usage:
#   ./explicit_casts_gen.sh > explicit_casts.csv

echo 'literal,type,expect'
while read -r type; do
  while read -r literal; do
    # Quote literal and type in case they contain quotes or commas.
    printf '"%s","%s",' "${literal//\"/\"\"}" "${type//\"/\"\"}"
    cast=$(printf '(%s)::%s' "$literal" "$type")
    psql --csv -qtc "SELECT quote_nullable($cast)" 2>/dev/null || echo 'error'
  done <literals.txt
done <types.txt
