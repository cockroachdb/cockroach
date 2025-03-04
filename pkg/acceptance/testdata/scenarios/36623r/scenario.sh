#!/usr/bin/env bash
set -eux

echo "While the decommission is ongoing, monitor for range imbalance."

time \
	$ROACHPROD run $CLUSTER:1 --tag run -- \
	"./cockroach node decommission $CENTER_MAPPING --insecure --wait all"
time \
	$ROACHPROD stop $CLUSTER:$CENTER_MAPPING --tag node
