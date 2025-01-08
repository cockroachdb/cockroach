#!/bin/bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# This script sets up the tpch run workload script in the workload nodes
# The --scale-factor and other flags are passed as argument to this script
# NOTE - This uses CLUSTER and WORKLOAD_CLUSTER environment variable, if not set the script fails
if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <script_suffix> <flags to init:--scale-factor,--db>"
  exit 1
fi
suffix=$1
shift

if [ -z "${CLUSTER}" ]; then
  echo "environment CLUSTER is not set"
  exit 1
fi

if [ -z "${WORKLOAD_CLUSTER}" ]; then
  echo "environment WORKLOAD_CLUSTER is not set"
  exit 1
fi

absolute_path=$(drtprod run "${WORKLOAD_CLUSTER}":1 -- "realpath ./cockroach")
pwd=$(drtprod run "${WORKLOAD_CLUSTER}":1 -- "dirname ${absolute_path}")
PGURLS=$(drtprod pgurl "${CLUSTER}":1)

drtprod ssh "${WORKLOAD_CLUSTER}":1 -- "tee tpch_run_${suffix}.sh > /dev/null << 'EOF'
#!/bin/bash

${pwd}/cockroach workload run tpch $@ --verbose --prometheus-port 2113 $PGURLS
EOF"
drtprod ssh "${WORKLOAD_CLUSTER}":1 -- "chmod +x tpch_run_${suffix}.sh"
