#!/usr/bin/env bash
set -euo pipefail

# give roach containers a few seconds to become ready
# N.B. not strictly required since init will retry, but otherwise reduces noise in the log
echo "Sleeping for 5s before init..."
sleep 5

# one time cluster initialization
./cockroach init --cluster-name=test --insecure --host=roach1

echo "CLUSTER 'test' has been inited; waiting until liveness..."

# wait until each node has updated its hearbeat in the liveness range
ready_roaches=0

while [ $ready_roaches -lt 3 ]; do
  roach1_liveness=$(./cockroach sql --insecure --url 'postgresql://root@roach1:26257?sslmode=disable' --execute "select membership from crdb_internal.kv_node_liveness" |grep active | wc -l);
  roach2_liveness=$(./cockroach sql --insecure --url 'postgresql://root@roach2:26257?sslmode=disable' --execute "select membership from crdb_internal.kv_node_liveness" |grep active | wc -l);
  roach3_liveness=$(./cockroach sql --insecure --url 'postgresql://root@roach3:26257?sslmode=disable' --execute "select membership from crdb_internal.kv_node_liveness" |grep active | wc -l);

  if [ $roach1_liveness -eq 3 ]; then
    ready_roaches=$(( $ready_roaches + 1 ))
  fi
  if [ $roach2_liveness -eq 3 ]; then
    ready_roaches=$(( $ready_roaches + 1 ))
  fi
  if [ $roach3_liveness -eq 3 ]; then
    ready_roaches=$(( $ready_roaches + 1 ))
  fi

  if [ $ready_roaches -lt 3 ]; then
    echo "Not all roaches are ready; $ready_roaches out of 3; sleeping..."
    sleep 1
  fi
done

echo "Executing workload to generate tables and data..."
./cockroach workload init bank 'postgresql://root@roach1:26257?sslmode=disable'

VMODULE="txn_coord_sender=5,dist_sender=5,cmd_end_transaction=2,cmd_push_txn=2,manager=2,lock_table_waiter=2,transaction=2,queue=2"
echo "Enabling verbose logging: VMODULE='$VMODULE'"
./cockroach sql --insecure --url 'postgresql://root@roach1:26257?sslmode=disable' --execute "SELECT crdb_internal.set_vmodule('$VMODULE')"

# N.B. we signal the fuzzer only after successfully initializing the workload.
#      In future, we could consider subjecting both (workload) data generation and loading to failures.
echo "READY FOR FUZZING..."

echo "Running bank workload..."
# N.B. we enable --tolerate-errors to ensure the workload can survive transient cluster unavailability (owing to failure injection).
./cockroach workload run bank --duration=5m 'postgresql://root@roach1:26257?sslmode=disable' --tolerate-errors
