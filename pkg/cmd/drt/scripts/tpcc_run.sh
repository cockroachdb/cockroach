#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -o pipefail

TPCC_DB=cct_tpcc
TPCC_USER=cct_tpcc_user
TPCC_PASSWORD=tpcc
PGURLS=$(./roachprod pgurl cct-232 --external --secure --cluster application | sed s/\'//g)

read -r -a PGURLS_ARR <<< "$PGURLS"

j=0
while true; do
    echo ">> Starting tpcc workload"
    ((j++))
    LOG=./tpcc_$j.txt
    ./cockroach workload run tpcc \
      --warehouses 3000 \
      --active-warehouses 1500 \
      --concurrency 128 \
      --max-rate 7000 \
      --db cct_tpcc \
      --secure \
      --ramp 10m \
      --display-every 5s \
      --duration 12h \
      --user cct_tpcc_user \
      --tolerate-errors \
      --password tpcc \
      --families \
        "${PGURLS_ARR[@]}" | tee $LOG
    if [ $? -eq 0 ]; then
        rm "$LOG"
    fi
    sleep 1
done
