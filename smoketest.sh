#!/bin/bash
set -euxo pipefail

#build/builder.sh mkrelease
roachprod destroy local || true
roachprod create -n 3 local
roachprod put local cockroach
roachprod start local:1-2
roachprod start local:3 -e 'COCKROACH_PEBBLE_COMPACTION_DELAY=10s' -e 'COCKROACH_DEBUG_PEBBLE_INGEST_L0=true'
sleep 60
roachprod run local -- ./cockroach workload fixtures import bank --payload-bytes=10240 --ranges=10 --rows=65104166 &
roachprod ssh local:3 -- tail -F logs/cockroach-pebble.log
