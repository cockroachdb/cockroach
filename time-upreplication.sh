#!/bin/bash

set -euo pipefail

roachprod destroy local || true
roachprod create -n 3 local
roachprod put local ./cockroach

mkdir -p $HOME/local/{1,2,3}/data

roachprod start local -a --store=type=mem,size=1G -e COCKROACH_SCAN_INTERVAL=1ms -e COCKROACH_SCAN_MAX_IDLE_TIME=1ms -e COCKROACH_SCAN_MIN_IDLE_TIME=1ms

time roachprod sql local:1 -- -e "
set cluster setting kv.raft_log.disable_synchronization_unsafe=true;
select crdb_internal.force_retry('3100ms') from crdb_internal.ranges_no_leases where array_length(replicas, 1) < 3;

select count(range_id) from crdb_internal.ranges_no_leases where array_length(replicas, 1) >= 3;
"

go test -c ./pkg/server
time ./server.test -test.run '^TestAdminAPITableStats'
