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

# Calculate group sizes using balanced distribution
# Unequal distribution
#
# For a cockroach cluster with 150 nodes and a workload cluster with 9 nodes,
# the larger and smaller size will be 17 and 16 respectively. We then calculate the group sizes
# for the larger and smaller sizes. For this example group 1 and 2 will be 6 and 3 respectively.
# Thus we have, 17*6 + 16*3 = 150
#
# Equal distribution
#
# For 150 nodes with 10 workload nodes larger and smaller size will be equal. Consequently,
# group 1 will be 0 and group 2 will be simply 150/10 = 15
LARGER_SIZE=$(( ($CLUSTER_NODES + $WORKLOAD_NODES - 1) / $WORKLOAD_NODES )) # Equivalent to ceil
SMALLER_SIZE=$(( $CLUSTER_NODES / $WORKLOAD_NODES ))

# Calculate size of groups
GROUP1=$(( $CLUSTER_NODES % $WORKLOAD_NODES ))
GROUP2=$(( $WORKLOAD_NODES - $GROUP1 ))

# Initialize offset
OFFSET=1

# Loop through each node
for NODE in $(seq 1 $WORKLOAD_NODES)
do
  # Calculate START_NODE_OFFSET and END_NODE_OFFSET
  if [ $NODE -le $GROUP1 ]; then
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

echo ">> Starting tpcc workload"
./cockroach workload run tpcc $@ \
    --ramp=10m \
    --wait=0 \
    --tolerate-errors \
    $PGURLS | tee ./tpcc_log.txt
EOF

  # Upload the script to the workload cluster
  roachprod put $WORKLOAD_CLUSTER:$NODE /tmp/tpcc_run.sh
  roachprod ssh $WORKLOAD_CLUSTER:$NODE -- chmod +x tpcc_run.sh
done
