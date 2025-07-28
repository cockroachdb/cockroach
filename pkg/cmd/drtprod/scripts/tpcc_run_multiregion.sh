#!/bin/bash
# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# This script sets up the tpcc multiregion run configuration on the workload node.

env_vars=(
  "CLUSTER"
  "WORKLOAD_CLUSTER"
  "NUM_REGIONS"
  "NODES_PER_REGION"
  "REGIONS"
  "TPCC_WAREHOUSES"
  "DB_NAME"
  "RUN_DURATION"
  "NUM_CONNECTIONS"
  "NUM_WORKERS"
  "MAX_RATE"
  "MAX_CONN_LIFETIME"
  "TPCC_ACTIVE_WAREHOUSES"
)
for var in "${env_vars[@]}"; do
  if [ -z "${!var}" ]; then
    echo "$var is not set"
    exit
  fi
done

export ROACHPROD_DISABLED_PROVIDERS=IBM

for NODE in $(seq 1 $NUM_REGIONS)
do
  NODE_OFFSET=$(($(($(($NODE - 1))*$NODES_PER_REGION))+1))
  LAST_NODE_IN_REGION=$(($NODE_OFFSET+$NODES_PER_REGION-1))
  # Since we're running a number of workers much smaller than the number of
  # warehouses, we have to do some strange math here. Workers are assigned to
  # warehouses in order (i.e. worker 1 will target warehouse 1). The
  # complication is that when we're partitioning the workload such that workers in
  # region 1 should only target warehouses in region 1, the workload binary will
  # not assign a worker if the warehouse is not in the specified region. As a
  # result, we must pass in a number of workers that is large enough to allow
  #  us to reach the specified region, and then add the actual number of workers
  #  we want to run.
  EFFECTIVE_NUM_WORKERS=$(($(($TPCC_WAREHOUSES/$NUM_REGIONS))*$(($NODE-1))+$NUM_WORKERS))
  cat <<EOF >/tmp/tpcc_run.sh
#!/usr/bin/env bash

export ROACHPROD_DISABLED_PROVIDERS=IBM
export ROACHPROD_GCE_DEFAULT_PROJECT=$ROACHPROD_GCE_DEFAULT_PROJECT
./roachprod sync
PGURLS=\$(./roachprod load-balancer pgurl $CLUSTER | sed s/\'//g)
if [ -z "\$PGURLS" ]; then
    echo ">> No load-balancer configured; falling back to direct pgurl"
    PGURLS=\$(./roachprod pgurl $CLUSTER | sed s/\'//g)
fi
read -r -a PGURLS_REGION <<< "\$PGURLS"

echo ">> Starting tpcc workload"
./cockroach workload run tpcc \
    --db=$DB_NAME \
    --warehouses=$TPCC_WAREHOUSES \
    --active-warehouses=$TPCC_ACTIVE_WAREHOUSES \
    --ramp=5m \
    --duration=$RUN_DURATION \
    --wait=true \
    --partitions=$NUM_REGIONS \
    --partition-affinity=$(($NODE-1)) \
    --tolerate-errors \
    --survival-goal region \
    --regions=$REGIONS \
    --max-conn-lifetime=$MAX_CONN_LIFETIME \
    --conns=$NUM_CONNECTIONS \
    --local-warehouses=true \
    \${PGURLS_REGION[@]}
EOF

  drtprod put $WORKLOAD_CLUSTER:$NODE /tmp/tpcc_run.sh
  drtprod ssh $WORKLOAD_CLUSTER:$NODE -- "chmod +x tpcc_run.sh"
done
