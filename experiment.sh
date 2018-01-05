#!/bin/bash

set -euxo pipefail

killall -9 cockroach || true
rm -rf cockroach-data

#make build

for i in 0 1 2; do
  ./cockroach start --insecure --max-offset=experimental-clockless --port $((26257+i)) --http-port $((8080+i)) --store cockroach-data/clockless$i --join :26257 &
  # uncomment for "regular mode"
  # ./cockroach start --insecure --port $((26257+i)) --http-port $((8080+i)) --store cockroach-data/clockless$i --join :26257 &
done

sleep 1

./cockroach init --insecure

go run pkg/testutils/workload/cmd/workload/*.go sillyseq \
  --concurrency 24 --duration 10s \
  --tolerate-errors \
  "postgres://root@localhost:26257?sslmode=disable postgres://root@localhost:26258?sslmode=disable postgres://root@localhost:26259?sslmode=disable"
