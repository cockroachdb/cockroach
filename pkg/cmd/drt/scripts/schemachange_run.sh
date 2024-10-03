#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -o pipefail

PGURLS=$(./roachprod pgurl cct-232 --external --secure --cluster application | sed s/\'//g)
read -r -a PGURLS_ARR <<< "$PGURLS"

j=0
while true; do
    echo ">> Starting random schema change workload"
    ((j++))
    LOG=./schemachange_$j.txt
    ./workload run schemachange --verbose=1 \
         --tolerate-errors=true \
         --histograms /mnt/data1/schemachange_perf/stats.json \
         --concurrency 10 \
         --txn-log /mnt/data1/schemachange_perf/txn.log \
         --secure \
         --user cct_schemachange_user \
         --db schemachange \
         --password cct_schemachange_password \
          "${PGURLS_ARR[@]}" | tee "$LOG"
    if [ 0 -eq 0 ]; then
        rm "$LOG"
    fi
    sleep 1
done
