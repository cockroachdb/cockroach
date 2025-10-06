#!/bin/bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# This script sets up the tpcc import workload script in the workload node and starts the same in nohup
# The --warehouses and other flags for import are passed as argument to this script
# NOTE - This uses CLUSTER and WORKLOAD_CLUSTER environment variable, if not set the script fails

if [ -z "${CLUSTER}" ]; then
  echo "environment CLUSTER is not set"
  exit 1
fi

if [ -z "${WORKLOAD_CLUSTER}" ]; then
  echo "environment CLUSTER is not set"
  exit 1
fi

absolute_path=$(roachprod run "${WORKLOAD_CLUSTER}":1 -- "realpath ./tpcc_init.sh")
pwd=$(roachprod run "${WORKLOAD_CLUSTER}":1 -- "dirname ${absolute_path}")

# script is responsible for importing the tpcc database for workload
roachprod ssh "${WORKLOAD_CLUSTER}":1 -- "tee tpcc_init.sh > /dev/null << 'EOF'
#!/bin/bash

export ROACHPROD_GCE_DEFAULT_PROJECT=${ROACHPROD_GCE_DEFAULT_PROJECT}
export ROACHPROD_DNS=${ROACHPROD_DNS}
${pwd}/roachprod sync
sleep 20
PGURLS=\$(${pwd}/roachprod pgurl ${CLUSTER} | sed s/\'//g)
${pwd}/cockroach workload init tpcc $@ --secure --families \$PGURLS
EOF"
roachprod ssh "${WORKLOAD_CLUSTER}":1 -- "chmod +x tpcc_init.sh"
roachprod run "${WORKLOAD_CLUSTER}":1 -- "sudo systemd-run --unit tpccinit --same-dir --uid \$(id -u) --gid \$(id -g) bash ${pwd}/tpcc_init.sh"
