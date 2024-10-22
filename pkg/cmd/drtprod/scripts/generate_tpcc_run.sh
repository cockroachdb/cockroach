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

if [ -z "${WORKLOAD_NODES}" ]; then
  echo "environment WORKLOAD_NODES is not set"
  exit 1
fi

# Prepare PGURLS
PGURLS=$(roachprod pgurl $CLUSTER | sed s/\'//g)

# Loop through each node
for NODE in $(seq 1 $WORKLOAD_NODES)
do
  # Create the workload script
  cat <<EOF >/tmp/tpcc_run.sh
#!/usr/bin/env bash

read -r -a PGURLS_ARR <<< "$PGURLS"

j=0
while true; do
    echo ">> Starting tpcc workload"
    ((j++))
    LOG=./tpcc_\$j.txt
    ./cockroach workload run tpcc $@ \
        --tolerate-errors \
        --families \
        "\${PGURLS_ARR[@]}" | tee \$LOG
    if [ \$? -eq 0 ]; then
        rm "\$LOG"
    fi
    sleep 1
done
EOF

  # Upload the script to the workload cluster
  roachprod put $WORKLOAD_CLUSTER:$NODE /tmp/tpcc_run.sh
  roachprod ssh $WORKLOAD_CLUSTER:$NODE -- chmod +x tpcc_run.sh
done
