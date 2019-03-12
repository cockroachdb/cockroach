#!/usr/bin/env bash
set -euo pipefail

export COCKROACH_ENGINE_MAX_SYNC_DURATION=60s

killall -9 cockroach || true
make build
rm -rf cockroach-data* || true
for i in 0 1 2 3; do
  ./cockroach start --max-offset 10ms --insecure --host 127.0.0.1 --port $((26257+i)) --http-port $((8080+i)) --background --store "cockroach-data${i}" --join 127.0.0.1:26257
  if [ $i -eq 0 ]; then ./cockroach init --insecure; fi
done
echo "
SET CLUSTER SETTING kv.range_merge.queue_interval = '1ns';
SET CLUSTER SETTING kv.range_merge.queue_enabled = false;
CREATE TABLE IF NOT EXISTS data (id INT PRIMARY KEY);
ALTER TABLE data SPLIT AT SELECT i FROM generate_series(1, 1000) AS g(i);
SET CLUSTER SETTING kv.range_merge.queue_enabled = true;
" | ./cockroach sql --insecure

./cockroach quit --insecure || true
sleep 1


retval=137
while [ $retval -eq 137 ];
do
  set +e
  ./cockroach start --max-offset 10ms --insecure --logtostderr --host 127.0.0.1 --store cockroach-data0
  retval=$?
  set -e
  echo "exit code: $retval"
done
