#!/bin/bash
set -euxo pipefail
./dev build short

roachprod wipe local
roachprod put local cockroach
roachprod start local
roachprod sql local:1 -- -e 'set cluster setting kv.range_split.load_qps_threshold = 9999999'
roachprod ssh local:1 -- ./cockroach workload init kv --splits 1

while true; do

roachprod ssh local:1 -- ./cockroach workload run kv --min-block-bytes 1024 --max-block-bytes 1024 --read-percent 0 {pgurl} --concurrency=4096 --tolerate-errors --duration 30s
roachprod monitor local --oneshot

done



