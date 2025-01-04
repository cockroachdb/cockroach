#!/bin/bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# This script sets up the tpcc run workload script in the workload nodes
# The --warehouses flag is passed as argument to this script
# NOTE - This uses CLUSTER and WORKLOAD_CLUSTER environment variable, if not set the script fails
if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <script_suffix> <execute:true|false> <flags to init:--warehouses,--db>"
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
  cat <<EOF >/tmp/tpcc_run_${suffix}.sh
#!/usr/bin/env bash

PGURLS=\$(./roachprod pgurl $CLUSTER:$START_OFFSET-$END_OFFSET | sed s/\'//g)
read -r -a PGURLS_ARR <<< "\$PGURLS"
j=0
while true; do
    echo ">> Starting tpcc workload"
    ((j++))
    LOG=./tpcc_\$j.txt
    ./cockroach workload run tpcc $@ \
        --tolerate-errors \
        --families \
         \${PGURLS_ARR[@]}  | tee \$LOG
    if [ \$? -eq 0 ]; then
        rm "\$LOG"
    fi
    sleep 1
done
EOF

#   Upload the script to the workload cluster
  drtprod put $WORKLOAD_CLUSTER:$((NODE + 1)) /tmp/tpcc_run_${suffix}.sh
  drtprod ssh $WORKLOAD_CLUSTER:$((NODE + 1)) -- "chmod +x tpcc_run_${suffix}.sh"
  if [ "$execute_script" = "true" ]; then
      drtprod run "${WORKLOAD_CLUSTER}":$((NODE + 1)) -- "sudo systemd-run --unit tpcc_run_${suffix} --same-dir --uid \$(id -u) --gid \$(id -g) bash ${pwd}/tpcc_run_${suffix}.sh"
  else
    echo "Run --> drtprod run "${WORKLOAD_CLUSTER}":$((NODE + 1)) -- \"sudo systemd-run --unit tpcc_run_${suffix} --same-dir --uid \\\$(id -u) --gid \\\$(id -g) bash ${pwd}/tpcc_run_${suffix}.sh\""
    fi
done
