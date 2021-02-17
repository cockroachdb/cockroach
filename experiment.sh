#!/bin/bash

set -euxo pipefail
#make build
clus=tobias-demo
#bin/roachprod create $clus -n 3 || true
bin/roachprod stop $clus || true
bin/roachprod put $clus ./cockroach-linux-2.6.32-gnu-amd64 ./cockroach
bin/roachprod start $clus
# for upreplication
sleep 5
bin/roachprod run $clus:1 -- ./cockroach workload init kv || true
bin/roachprod run $clus:1 -- ./cockroach workload init tpcc --warehouses 50 --data-loader=IMPORT || true
echo go tool pprof 'http://localhost:26262/debug/pprof/profile?seconds=10'
ulimit -n 65535
bin/roachprod run $clus:4 -- ./cockroach workload run kv --read-percent 50 --concurrency 10000 {pgurl} &
bin/roachprod run $clus:4 -- ./cockroach workload run tpcc --concurrency 1000 --conns 1000 --wait=false --mix=newOrder=1 {pgurl} 
