#!/bin/bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# Sets up TPCC workload script across nodes with optional client side data partitioning.
# Usage: <script_suffix> <execute:true|false> [workload flags]
# Requires: CLUSTER, WORKLOAD_CLUSTER, WORKLOAD_NODES env vars
# Optional: TOTAL_PARTITIONS (defaults to WORKLOAD_NODES if WORKLOAD_NODES>1)
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

# Set TOTAL_PARTITIONS equal to WORKLOAD_NODES if not set
if [ -z "${TOTAL_PARTITIONS}" ] && [ "${WORKLOAD_NODES}" -gt 1 ]; then
  TOTAL_PARTITIONS=$WORKLOAD_NODES
  echo "TOTAL_PARTITIONS not set and WORKLOAD_NODES > 1, defaulting to WORKLOAD_NODES ($WORKLOAD_NODES)"
fi

# Ensure TOTAL_PARTITIONS is not less than WORKLOAD_NODES
if [ -n "${TOTAL_PARTITIONS}" ] && [ "${TOTAL_PARTITIONS}" -lt "${WORKLOAD_NODES}" ]; then
  echo "TOTAL_PARTITIONS ($TOTAL_PARTITIONS) must be greater than or equal to WORKLOAD_NODES ($WORKLOAD_NODES)"
  exit 1
fi

export ROACHPROD_DISABLED_PROVIDERS=IBM

get_partitions_in_range() {
    local start=$(($1 - 1))
    local end=$(($2 - 1))

    if [[ -z "$start" || -z "$end" || "$start" -lt 0 || "$end" -ge $TOTAL_PARTITIONS || "$start" -gt "$end" ]]; then
        echo "Invalid range. Provide values between 0 and $((TOTAL_PARTITIONS - 1)), with start <= end."
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

for ((NODE=0; NODE<WORKLOAD_NODES; NODE++)); do
  # Handle partitioning if TOTAL_PARTITIONS is set
  partition_args=""
  if [ -n "${TOTAL_PARTITIONS}" ]; then
    PARTITION_PER_NODE=$((TOTAL_PARTITIONS / WORKLOAD_NODES))
    PARTITION_REMAINDER_NODE=$((TOTAL_PARTITIONS % WORKLOAD_NODES))

    PARTITION_START_OFFSET=$((NODE * PARTITION_PER_NODE + (NODE < PARTITION_REMAINDER_NODE ? NODE : PARTITION_REMAINDER_NODE) + 1))
    PARTITION_END_OFFSET=$((PARTITION_START_OFFSET + PARTITION_PER_NODE + (NODE < PARTITION_REMAINDER_NODE ? 1 : 0) - 1))

    parts=$(get_partitions_in_range ${PARTITION_START_OFFSET} ${PARTITION_END_OFFSET})
    if [ $? -ne 0 ]; then
        echo "Failed to get partitions in range"
        exit 1
    fi
    echo "Partition assignment: $parts"
    partition_args="--client-partitions=$TOTAL_PARTITIONS --partition-affinity=$parts"
  fi

  # Create the workload script
  cat <<EOF >/tmp/tpcc_run_${suffix}.sh
#!/usr/bin/env bash

export ROACHPROD_DISABLED_PROVIDERS=IBM
export ROACHPROD_GCE_DEFAULT_PROJECT=$ROACHPROD_GCE_DEFAULT_PROJECT
./drtprod sync
$([ "$execute_script" = "true" ] && [ "$NODE" -eq 0 ] && echo "${pwd}/tpcc_init_${suffix}.sh")
PGURLS=\$(./drtprod load-balancer pgurl $CLUSTER | sed s/\'//g)
if [ -z "\$PGURLS" ]; then
    echo ">> No load-balancer configured; falling back to direct pgurl"
    PGURLS=\$(./drtprod pgurl $CLUSTER | sed s/\'//g)
fi
read -r -a PGURLS_ARR <<< "\$PGURLS"
j=0
while true; do
    echo ">> Starting tpcc workload"
    ((j++))
    LOG=./tpcc_\$j.txt
    ./cockroach workload run tpcc $@ \
        $partition_args \
        --tolerate-errors \
        --families \
         \${PGURLS_ARR[@]}  | tee \$LOG
    if [ \$? -eq 0 ]; then
        rm "\$LOG"
    fi
    sleep 1
done
EOF

  # Upload the script to the workload cluster
  drtprod put $WORKLOAD_CLUSTER:$((NODE + 1)) /tmp/tpcc_run_${suffix}.sh
  drtprod ssh $WORKLOAD_CLUSTER:$((NODE + 1)) -- "chmod +x tpcc_run_${suffix}.sh"
  if [ "$execute_script" = "true" ]; then
      drtprod run "${WORKLOAD_CLUSTER}":$((NODE + 1)) -- "sudo systemd-run --unit tpcc_run_${suffix} --same-dir --uid \$(id -u) --gid \$(id -g) bash ${pwd}/tpcc_run_${suffix}.sh"
  else
    echo "Run --> drtprod run "${WORKLOAD_CLUSTER}":$((NODE + 1)) -- \"sudo systemd-run --unit tpcc_run_${suffix} --same-dir --uid \\\$(id -u) --gid \\\$(id -g) bash ${pwd}/tpcc_run_${suffix}.sh\""
  fi
done
