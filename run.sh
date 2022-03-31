#!/bin/bash
set -euxo pipefail

# roachprod create -n 3 tobias-ingest
roachprod wipe tobias-ingest
git submodule update
build/builder.sh mkrelease
roachprod run tobias-ingest -- rm -f cockroach
roachprod put tobias-ingest cockroach-linux-2.6.32-gnu-amd64 ./cockroach
roachprod start tobias-ingest:1-2
roachprod start tobias-ingest:3 -e 'COCKROACH_DEBUG_EXTRA_PRE_INGEST_DELAY=3s' -e 'COCKROACH_SCHEDULER_CONCURRENCY=2000'
sleep 60 # wait for replication
roachprod run tobias-ingest:3 -- ./cockroach node drain --self --insecure
# NB: sometimes n3 still has 1-2 leases at this point. Another drain fixes it.
sleep 5
roachprod run tobias-ingest:3 -- ./cockroach node drain --self --insecure
roachprod run tobias-ingest:1 -- ./cockroach workload fixtures import bank --payload-bytes=10240 --ranges=10 --rows=65104166 &
# roachprod ssh tobias-ingest:3 -- tail -F logs/cockroach-pebble.log
echo "$(roachprod adminui tobias-ingest:1)/#/debug/chart?charts=%5B%7B%22metrics%22%3A%5B%7B%22downsampler%22%3A1%2C%22aggregator%22%3A2%2C%22derivative%22%3A0%2C%22perNode%22%3Atrue%2C%22source%22%3A%22%22%2C%22metric%22%3A%22cr.store.storage.l0-num-files%22%7D%5D%2C%22axisUnits%22%3A0%7D%2C%7B%22metrics%22%3A%5B%7B%22downsampler%22%3A1%2C%22aggregator%22%3A2%2C%22derivative%22%3A0%2C%22perNode%22%3Atrue%2C%22source%22%3A%22%22%2C%22metric%22%3A%22cr.store.storage.l0-sublevels%22%7D%5D%2C%22axisUnits%22%3A0%7D%5D"
