#!/bin/bash
set -euxo pipefail
./dev build short
roachprod wipe local
roachprod put local cockroach
roachprod start local
roachprod sql local:1 -- -e 'set cluster setting kv.range_split.load_qps_threshold = 9999999'
roachprod ssh local:1 -- ./cockroach workload init kv --splits 1
roachprod ssh local:1 -- ./cockroach workload run kv --read-percent 0 {pgurl} --concurrency 512
