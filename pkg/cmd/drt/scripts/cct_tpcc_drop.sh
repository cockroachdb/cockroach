#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


CLUSTER_NAME=cct-232
PG_URL_N1=$(./roachprod pgurl $CLUSTER_NAME:1 --secure --cluster=application --external | sed s/\'//g)
PGURLS=$(./roachprod pgurl cct-232 --external --secure --cluster application | sed s/\'//g)

read -r -a PGURLS_ARR <<< "$PGURLS"

echo ">> Dropping old databases if they exist"
./cockroach sql --url "${PG_URL_N1}" -e "DROP DATABASE IF EXISTS cct_tpcc_drop_old CASCADE;"
./cockroach sql --url "${PG_URL_N1}" -e "DROP DATABASE IF EXISTS cct_tpcc_drop CASCADE;"

j=0
while true; do
    echo ">> Starting tpcc-drop-db workload"
    echo ">> Importing tpcc"
    ((j++))
    INIT_LOG=./cct_tpcc_drop_init_$j.txt
    RUN_LOG=./cct_tpcc_drop_run_$j.txt

    # Temporarily, we cleanup and paused IMPORT jobs that may exist due to
    # node failures. Long-term, we will address these failures by making
    # IMPORT more resilient and/or creating a variant of IMPORT which cancels
    # itself instead of pausing.
    ./cockroach sql --url "${PG_URL_N1}" -e "CANCEL JOBS (WITH x AS (SHOW JOBS) SELECT job_id FROM x WHERE status = 'paused' AND job_type = 'IMPORT');"
    sleep 15

    ./cockroach workload init tpcc \
        --warehouses=3000 \
        --secure \
  --concurrency 4 \
        --db cct_tpcc_drop \
        $PG_URL_N1 | tee "$INIT_LOG"
    echo ">> Dropping cct_tpcc_drop_old if it exists"
    ./cockroach sql --url "${PG_URL_N1}" -e "DROP DATABASE cct_tpcc_drop_old CASCADE;"
    sleep 5
    echo ">> Starting tpcc workload for 1h"
    ./cockroach workload run tpcc \
        --warehouses 3000 \
        --active-warehouses 1000 \
        --db cct_tpcc_drop \
        --secure \
  --prometheus-port 2113 \
        --ramp 5m \
        --display-every 5s \
        --duration 60m \
  --tolerate-errors \
   "${PGURLS_ARR[@]}" | tee "$RUN_LOG"

    echo ">> Renaming to cct_tpcc_drop_old"
    ./cockroach sql --url "${PG_URL_N1}" -e "ALTER DATABASE cct_tpcc_drop RENAME TO cct_tpcc_drop_old;"

    sleep 1
done
