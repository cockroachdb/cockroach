#!/bin/bash

set -euxo pipefail
make build
bin/roachprod create local -n 3 || true
bin/roachprod put local ./cockroach ./cockroach
bin/roachprod stop local || true
bin/roachprod start local
# for upreplication
sleep 10
./cockroach workload init kv || true
./cockroach workload init tpcc --warehouses 50 --data-loader=IMPORT || true
echo go tool pprof 'http://localhost:26262/debug/pprof/profile?seconds=10'
ulimit -n 65535
bin/roachprod run local:1 -- ./cockroach workload run kv --read-percent 50 --concurrency 10000 {pgurl} &
bin/roachprod run local:1 -- ./cockroach workload run tpcc --concurrency 1000 --conns 1000 --wait=false --mix=newOrder=1 {pgurl} &
