#!/bin/bash
set -euxo pipefail

# roachprod create -n 3 tobias-ingest
roachprod wipe tobias-ingest
build/builder.sh mkrelease
roachprod run tobias-ingest -- rm -f cockroach
roachprod put tobias-ingest cockroach
roachprod start tobias-ingest:1-2
roachprod start tobias-ingest:3 -e 'COCKROACH_PEBBLE_COMPACTION_DELAY=10s'
roachprod run tobias-ingest:1 -- ./cockroach workload fixtures import bank --payload-bytes=10240 --ranges=10 --rows=65104166 &
roachprod ssh tobias-ingest:3 -- tail -F logs/cockroach-pebble.log
