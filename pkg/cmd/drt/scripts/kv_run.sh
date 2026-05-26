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
    echo ">> Starting kv workload"
    ((j++))
    LOG=./kv_$j.txt
    ./cockroach workload run kv \
        --init \
        --drop \
        --concurrency 128 \
        --histograms kv/stats.json \
        --db kv \
        --splits 1000 \
        --read-percent 50 \
        --span-percent 20 \
        --cycle-length 100000 \
        --min-block-bytes 100 \
        --max-block-bytes 1000 \
        --prometheus-port 2114 \
        --max-rate 1200 \
        --secure \
        --ramp 10m \
        --display-every 5s \
        --duration 12h \
        --tolerate-errors \
        --enum \
        $PGURLS | tee "$LOG"
    if [ $? -eq 0 ]; then
        rm "$LOG"
    fi
    sleep 1
done
