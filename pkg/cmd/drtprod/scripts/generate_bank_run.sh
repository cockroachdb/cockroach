#!/bin/bash

# Copyright 2026 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# This script sets up the bank workload run script on the workload nodes.
# The bank workload transfers random amounts between accounts and can be
# checked for consistency via `cockroach workload check bank`.
# NOTE - This uses CLUSTER, WORKLOAD_CLUSTER, and WORKLOAD_NODES environment
# variables; if not set the script fails.

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <script_suffix> <execute:true|false> [flags to run: --concurrency, --max-rate, etc.]"
  exit 1
fi
suffix=$1
shift
if [ "$1" != "true" ] && [ "$1" != "false" ]; then
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

export ROACHPROD_DISABLED_PROVIDERS=IBM

absolute_path=$(drtprod run "${WORKLOAD_CLUSTER}":1 -- "realpath ./cockroach")
workload_dir=$(drtprod run "${WORKLOAD_CLUSTER}":1 -- "dirname ${absolute_path}")
PGURLS=$(drtprod pgurl $CLUSTER --external | sed s/\'//g)

# Loop through each workload node
for NODE in $(seq 1 $WORKLOAD_NODES)
do
  # Create the workload script
  cat <<EOF >/tmp/bank_run_${suffix}.sh
#!/usr/bin/env bash

read -r -a PGURLS_ARR <<< "$PGURLS"

j=0
while true; do
    echo ">> Starting bank workload"
    ((j++))
    LOG=./bank_\$j.txt
    ./cockroach workload run bank \
        --histograms bank/stats.json \
        --tolerate-errors \
        --display-every 5s \
        --duration 12h \
        "$@" \
        "\${PGURLS_ARR[@]}" | tee "\$LOG"
    if [ \$? -eq 0 ]; then
        rm "\$LOG"
    fi
    sleep 1
done
EOF

  # Upload the script to the workload cluster
  drtprod put $WORKLOAD_CLUSTER:$NODE /tmp/bank_run_${suffix}.sh
  drtprod ssh $WORKLOAD_CLUSTER:$NODE -- "chmod +x bank_run_${suffix}.sh"
done

if [ "$execute_script" = "true" ]; then
  drtprod run "${WORKLOAD_CLUSTER}":1 -- "sudo systemd-run --unit bank_run_${suffix} --same-dir --uid \$(id -u) --gid \$(id -g) bash ${workload_dir}/bank_run_${suffix}.sh"
else
  echo "Run --> drtprod run "${WORKLOAD_CLUSTER}":1 -- \"sudo systemd-run --unit bank_run_${suffix} --same-dir --uid \\\$(id -u) --gid \\\$(id -g) bash ${workload_dir}/bank_run_${suffix}.sh\""
fi
