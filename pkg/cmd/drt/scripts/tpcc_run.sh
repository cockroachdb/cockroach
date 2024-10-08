#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -o pipefail

TPCC_DB=cct_tpcc
TPCC_USER=cct_tpcc_user
TPCC_PASSWORD=tpcc
export ROACHPROD_GCE_DEFAULT_PROJECT=cockroach-drt
export ROACHPROD_DNS="drt.crdb.io"
./roachprod sync
sleep 20
PGURLS=$(./roachprod pgurl drt-scale | sed s/\'//g)

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
      $PGURLS | tee $LOG
    if [ $? -eq 0 ]; then
        rm "$LOG"
    fi
    sleep 1
done
