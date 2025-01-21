#!/bin/bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# This script sets up the ycsb import workload script in the workload node and starts the same in nohup
# The --warehouses and other flags for import are passed as argument to this script
# NOTE - This uses CLUSTER and WORKLOAD_CLUSTER environment variable, if not set the script fails

# The first argument is the name suffix that is added to the script as ycsb_init_<suffix>.sh
if [ "$#" -lt 4 ]; then
  echo "Usage: $0 <script_suffix> <execute:true|false> <flags to init:--splits, --insert-count>"
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
  echo "environment CLUSTER is not set"
  exit 1
fi

absolute_path=$(drtprod run "${WORKLOAD_CLUSTER}":1 -- "realpath ./cockroach")
pwd=$(drtprod run "${WORKLOAD_CLUSTER}":1 -- "dirname ${absolute_path}")
PGURLS=$(drtprod pgurl "${CLUSTER}":1)

# script is responsible for importing the ycsb database for workload
drtprod ssh "${WORKLOAD_CLUSTER}":1 -- "tee ycsb_init_${suffix}.sh > /dev/null << 'EOF'
#!/bin/bash

${pwd}/cockroach workload init ycsb $PGURLS --drop --families=false $@
EOF"
drtprod ssh "${WORKLOAD_CLUSTER}":1 -- "chmod +x ycsb_init_${suffix}.sh"

if [ "$execute_script" = "true" ]; then
  drtprod run "${WORKLOAD_CLUSTER}":1 -- "sudo systemd-run --unit ycsb_init_${suffix} --same-dir --uid \$(id -u) --gid \$(id -g) bash ${pwd}/ycsb_init_${suffix}.sh"
else
  echo "Run --> drtprod run "${WORKLOAD_CLUSTER}":1 -- \"sudo systemd-run --unit ycsb_init_${suffix} --same-dir --uid \\\$(id -u) --gid \\\$(id -g) bash ${pwd}/ycsb_init_${suffix}.sh\""
fi
