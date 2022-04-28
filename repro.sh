#!/bin/bash

set -euo pipefail

roachprod destroy local || true
roachprod create -n 3 local
roachprod put local ./cockroach

roachprod start local:1

./cockroach workload init kv

# Wait for initial splits
sleep 20

./cockroach workload run kv --min-block-bytes 1024 --max-block-bytes 1024 --batch 1024 --max-ops 100 --sequential --read-percent 0

# Should print 45
./cockroach sql --insecure -e "select range_id from crdb_internal.ranges_no_leases where table_name = 'kv';"

roachprod start local:2-3

(cat ~/local/*/logs/cockroach.log && tail -F ~/local/*/logs/cockroach.log) | grep --line-buffered -E 'XXX|YYY' | tee log.txt | grep -F r45/

