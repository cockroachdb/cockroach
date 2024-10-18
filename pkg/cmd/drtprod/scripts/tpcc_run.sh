#!/bin/bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# This script sets up the tpcc run workload script in the workload nodes
# The --warehouses flag is passed as argument to this script
# NOTE - This uses CLUSTER and WORKLOAD_CLUSTER environment variable, if not set the script fails

if [ -z "${CLUSTER}" ]; then
  echo "environment CLUSTER is not set"
  exit 1
fi

if [ -z "${WORKLOAD_CLUSTER}" ]; then
  echo "environment WORKLOAD_CLUSTER is not set"
  exit 1
fi

if [ -z "${CLUSTER_NODES}" ]; then
  echo "environment CLUSTER_NODES is not set"
  exit 1
fi

if [ -z "${WORKLOAD_NODES}" ]; then
  echo "environment WORKLOAD_NODES is not set"
  exit 1
fi

# Calculate group sizes
LARGER_SIZE=$(( ($CLUSTER_NODES + $WORKLOAD_NODES - 1) / $WORKLOAD_NODES )) # Equivalent to ceil
SMALLER_SIZE=$(( $CLUSTER_NODES / $WORKLOAD_NODES ))

# Calculate number of larger and smaller groups
NUM_LARGER=$(( $CLUSTER_NODES % $WORKLOAD_NODES ))
NUM_SMALLER=$(( $WORKLOAD_NODES - NUM_LARGER ))

# Initialize offset
OFFSET=1

# Loop through each node
for NODE in $(seq 1 $WORKLOAD_NODES)
do
  # Calculate START_NODE_OFFSET and END_NODE_OFFSET
  if [ $NODE -le $NUM_LARGER ]; then
    START_NODE_OFFSET=$OFFSET
    END_NODE_OFFSET=$((START_NODE_OFFSET + LARGER_SIZE - 1))
    OFFSET=$((OFFSET + LARGER_SIZE))
  else
    START_NODE_OFFSET=$OFFSET
    END_NODE_OFFSET=$((START_NODE_OFFSET + SMALLER_SIZE - 1))
    OFFSET=$((OFFSET + SMALLER_SIZE))
  fi

  # Prepare PGURLS
  PGURLS=$(roachprod pgurl $CLUSTER:$START_NODE_OFFSET-$END_NODE_OFFSET | sed s/\'//g)

  # Create the workload script
  cat <<EOF >/tmp/tpcc_run.sh
#!/usr/bin/env bash

j=0
while true; do
  echo ">> Starting tpcc workload"
  ((j++))
  LOG=./tpcc_\$j.txt
  ./workload run tpcc $@ \
      --ramp=10m \
      --wait=false \
      --partition-affinity=$NODE \
      --tolerate-errors \
      $PGURLS | tee \$LOG
done
EOF

  # Upload the script to the workload cluster
  roachprod put $WORKLOAD_CLUSTER:$NODE /tmp/tpcc_run.sh
  roachprod ssh $WORKLOAD_CLUSTER:$NODE -- chmod +x tpcc_run.sh
done
