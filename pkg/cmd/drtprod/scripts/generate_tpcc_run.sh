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

get_partitions_in_range() {
    local start=$(($1 - 1))
    local end=$(($2 - 1))

    if [[ -z "$start" || -z "$end" || "$start" -lt 0 || "$end" -ge $WORKLOAD_NODES || "$start" -gt "$end" ]]; then
        echo "Invalid range. Provide values between 0 and $((WORKLOAD_NODES - 1)), with start <= end."
        return 1
    fi

    local result=""
    for (( i=start; i<=end; i++ )); do
        if [ -z "$result" ]; then
            result="$i"
        else
            result="${result},$i"
        fi
    done

    echo "$result"
}

absolute_path=$(drtprod run "${WORKLOAD_CLUSTER}":1 -- "realpath ./cockroach")
pwd=$(drtprod run "${WORKLOAD_CLUSTER}":1 -- "dirname ${absolute_path}")

# Calculate the number of PGURLS each workload node should get
PGURL_PER_NODE=$((CLUSTER_NODES / WORKLOAD_NODES))
REMAINDER_NODE=$((CLUSTER_NODES % WORKLOAD_NODES))
PARTITION_PER_NODE=$((WORKLOAD_NODES / WORKLOAD_NODES))
PARTITION_REMAINDER_NODE=$((WORKLOAD_NODES % WORKLOAD_NODES))

# Distribute the PGURLS among the workload nodes
for ((NODE=0; NODE<WORKLOAD_NODES; NODE++)); do
#  START_OFFSET=$((NODE * PGURL_PER_NODE + (NODE < REMAINDER_NODE ? NODE : REMAINDER_NODE) + 1))
#  END_OFFSET=$((START_OFFSET + PGURL_PER_NODE + (NODE < REMAINDER_NODE ? 1 : 0) - 1))

  PARTITION_START_OFFSET=$((NODE * PARTITION_PER_NODE + (NODE < PARTITION_REMAINDER_NODE ? NODE : PARTITION_REMAINDER_NODE) + 1))
  PARTITION_END_OFFSET=$((PARTITION_START_OFFSET + PARTITION_PER_NODE + (NODE < PARTITION_REMAINDER_NODE ? 1 : 0) - 1))

  # Print or use the PGURLS for the current workload node
#  echo "pgurl for Nodes ${START_OFFSET}:${END_OFFSET}"
  parts=$(get_partitions_in_range ${PARTITION_START_OFFSET} ${PARTITION_END_OFFSET})
  if [ $? -ne 0 ]; then
      echo "Failed to get partitions in range"
      exit 1
  fi
  echo "$parts"

  # Create the workload script
  cat <<EOF >/tmp/tpcc_run_${suffix}.sh
#!/usr/bin/env bash

./drtprod sync
PGURLS=\$(./drtprod pgurl $CLUSTER | sed s/\'//g)
read -r -a PGURLS_ARR <<< "\$PGURLS"
j=0
while true; do
    echo ">> Starting tpcc workload"
    ((j++))
    LOG=./tpcc_\$j.txt
    ./cockroach workload run tpcc $@ \
        --client-partitions=$WORKLOAD_NODES \
        --partition-affinity=$parts \
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
