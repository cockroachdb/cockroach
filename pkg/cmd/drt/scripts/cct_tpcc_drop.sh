#!/usr/bin/env bash

CLUSTER_NAME=cct-232
PG_URL_N1=$(roachprod pgurl $CLUSTER_NAME:1 --secure)
PG_URLS=$(roachprod pgurl $CLUSTER_NAME --secure)

j=0
while true; do
    echo ">> Starting tpcc-drop-db workload"
    echo ">> Importing tpcc"
    ((j++))
    INIT_LOG=./cct_tpcc_drop_init_$j.txt
    RUN_LOG=./cct_tpcc_drop_run_$j.txt
    ./cockroach workload init tpcc \
        --warehouses=3000 \
        --secure \
  --concurrency 4 \
        --db cct_tpcc_drop \
        $PG_URL_N1 | tee "$INIT_LOG"
    echo ">> Dropping cct_tpcc_drop_old if it exists"
    ./cockroach sql --url $PG_URL_N1 -e "DROP DATABASE cct_tpcc_drop_old CASCADE;"
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
   $PG_URLS| tee "$RUN_LOG"

    echo ">> Renaming to cct_tpcc_drop_old"
    ./cockroach sql --url $PG_URLS -e "ALTER DATABASE cct_tpcc_drop RENAME TO cct_tpcc_drop_old;"

    sleep 1
done
