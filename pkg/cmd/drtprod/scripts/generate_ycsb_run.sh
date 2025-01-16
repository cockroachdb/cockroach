#!/bin/bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# This script sets up the ycsb run workload script in the workload nodes
# The --warehouses flag is passed as argument to this script
# NOTE - This uses CLUSTER and WORKLOAD_CLUSTER environment variable, if not set the script fails
if [ "$#" -lt 7 ]; then
  echo "Usage: $0 <script_suffix> <execute:true|false> <flags to run: max-rate, read-freq, insert-freq, update-freq, delete-freq>"
  exit 1
fi
suffix=$1
shift
# The second argument represents whether the init process should be started in the workload cluster
# The value is true or false
if [ "$1" != "true" ] && [ "$1" != "false" ]; then
  # $1 is used again because of the shift
  echo "Error: The second argument must be 'true' or 'false' which implies whether the script should be started in background or not."
  exit 1
fi
execute_script=$1
shift

if [ -z "${CLUSTER}" ]; then
  echo "environment CLUSTER is not set"
  exit 1
fi

if [ -z "${WORKLOAD_CLUSTER}" ]; then
  echo "environment WORKLOAD_CLUSTER is not set"
  exit 1
fi

if [ -z "${WORKLOAD_NODES}" ]; then
  echo "environment WORKLOAD_NODES is not set"
  exit 1
fi

if [ -z "${CLUSTER_NODES}" ]; then
  echo "environment CLUSTER_NODES is not set"
  exit 1
fi

absolute_path=$(drtprod run "${WORKLOAD_CLUSTER}":1 -- "realpath ./cockroach")
pwd=$(drtprod run "${WORKLOAD_CLUSTER}":1 -- "dirname ${absolute_path}")

# Calculate the number of PGURLS each workload node should get
PGURL_PER_NODE=$((CLUSTER_NODES / WORKLOAD_NODES))
REMAINDER_NODE=$((CLUSTER_NODES % WORKLOAD_NODES))

# Distribute the PGURLS among the workload nodes
for ((NODE=0; NODE<WORKLOAD_NODES; NODE++)); do
  START_OFFSET=$((NODE * PGURL_PER_NODE + (NODE < REMAINDER_NODE ? NODE : REMAINDER_NODE) + 1))
  END_OFFSET=$((START_OFFSET + PGURL_PER_NODE + (NODE < REMAINDER_NODE ? 1 : 0) - 1))

  # Print or use the PGURLS for the current workload node
  echo "pgurl for Nodes ${START_OFFSET}:${END_OFFSET}"

  # Create the workload script
  cat <<EOF >/tmp/ycsb_run_${suffix}.sh
#!/usr/bin/env bash

export ROACHPROD_GCE_DEFAULT_PROJECT=$ROACHPROD_GCE_DEFAULT_PROJECT
INSERT_START=10000000000000000
NUM_WORKERS_PER_NODE=5
OUTPUT_FILE_A="ycsb-a-\$(date '+%Y-%m-%d-%H:%M:%S').log"
OUTPUT_ERROR_FILE_A="ycsb-a-\$(date '+%Y-%m-%d-%H:%M:%S').error.log"
CLIENTS_PER_WORKLOAD=4000

./roachprod sync
PGURLS=\$(./roachprod pgurl $CLUSTER:$START_OFFSET-$END_OFFSET | sed s/\'//g)
read -r -a PGURLS_ARR <<< "\$PGURLS"

for ((j=1;j<=\$NUM_WORKERS_PER_NODE;j++)); do
    echo ">> Starting ycsb workload"
    nohup ./cockroach workload run ycsb --tolerate-errors --workload='custom' \
       --min-conns=\$((CLIENTS_PER_WORKLOAD/NUM_WORKERS_PER_NODE)) $@ \
       --insert-start=\$((INSERT_START*$NODE+(INSERT_START/j))) \
        --families=false --request-distribution='uniform' --scan-length-distribution='uniform' \
         --concurrency=\$((CLIENTS_PER_WORKLOAD/NUM_WORKERS_PER_NODE)) \
          \${PGURLS_ARR[@]} > \$OUTPUT_FILE_A 2> \$OUTPUT_ERROR_FILE_A &
done
EOF

#   Upload the script to the workload cluster
  drtprod put $WORKLOAD_CLUSTER:$((NODE + 1)) /tmp/ycsb_run_${suffix}.sh
  drtprod ssh $WORKLOAD_CLUSTER:$((NODE + 1)) -- "chmod +x ycsb_run_${suffix}.sh"
done
if [ "$execute_script" = "true" ]; then
    drtprod run "${WORKLOAD_CLUSTER}" -- "${pwd}/ycsb_run_${suffix}.sh"
else
  echo "Run --> drtprod run "${WORKLOAD_CLUSTER}" -- \"${pwd}/ycsb_run_${suffix}.sh\""
fi
