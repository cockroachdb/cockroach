#!/bin/bash

set -euo pipefail

roachprod destroy local || true
roachprod create -n 5 local

roachprod put local cockroach
roachprod start local:1-5
sleep 60
roachprod run local:1 -- ./cockroach workload init kv --splits 100
roachprod run local:1 -- ./cockroach workload run kv --max-rate 100 --read-percent 0 --duration=10s {pgurl:1-5}

roachprod sql local:1 -- -e 'set cluster setting kv.replica_circuit_breakers.enabled = true'

roachprod stop local:1,2

roachprod run local:1 -- ./cockroach workload run kv --max-rate 100 --read-percent 0 --tolerate-errors --duration=60000s {pgurl:3-5} 2> errors.txt

# TODO: sometimes don't get 100 errors/sec but we get stuck somewhere, use this
# below to chase down the ranges and to look into it. Consider filtering on
# numPending > 0 to get the more interesting ones.
# curl http://localhost:26264/_status/ranges/local | jq '.ranges[] | select((.state.circuitBreakerError == "") and (.span.startKey | test("Table/56")))' | less

# TODO: problem ranges should highlight circ breakers
# TODO: careful with whole-cluster restarts (or restarts of nodes that are required for quorum); is there any KV access in the start path that wll fatal the node on a hard error (such as an open circuit breaker error from somewhere?)
