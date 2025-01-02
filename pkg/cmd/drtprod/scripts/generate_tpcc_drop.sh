#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# This script sets up the tpcc drop workload script in the workload nodes
# NOTE - This uses CLUSTER and WORKLOAD_CLUSTER environment variable, if not set the script fails
if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <execute:true|false>"
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

PG_URL_N1=$(drtprod pgurl $CLUSTER:1  --external | sed s/\'//g)
PGURLS=$(drtprod pgurl $CLUSTER --external  | sed s/\'//g)

# Loop through each node
for NODE in $(seq 1 $WORKLOAD_NODES)
do
  # Create the workload script
  cat <<EOF >/tmp/tpcc_drop.sh
#!/usr/bin/env bash

read -r -a PGURLS_ARR <<< "$PGURLS"

echo ">> Dropping old databases if they exist"
./cockroach sql --url "${PG_URL_N1}" -e "DROP DATABASE IF EXISTS cct_tpcc_drop_old CASCADE;"
./cockroach sql --url "${PG_URL_N1}" -e "DROP DATABASE IF EXISTS cct_tpcc_drop CASCADE;"

j=0
while true; do
    echo ">> Starting tpcc-drop-db workload"
    echo ">> Importing tpcc"
    ((j++))
    INIT_LOG=./cct_tpcc_drop_init_\$j.txt
    RUN_LOG=./cct_tpcc_drop_run_\$j.txt

    # Temporarily, we cleanup and paused IMPORT jobs that may exist due to
    # node failures. Long-term, we will address these failures by making
    # IMPORT more resilient and/or creating a variant of IMPORT which cancels
    # itself instead of pausing.
    ./cockroach sql --url "${PG_URL_N1}" -e "CANCEL JOBS (WITH x AS (SHOW JOBS) SELECT job_id FROM x WHERE status = 'paused' AND job_type = 'IMPORT');"
    sleep 15

    ./cockroach workload init tpcc \
        --warehouses=3000 \
        --secure \
  --concurrency 4 \
        --db cct_tpcc_drop \
        \$PG_URL_N1 | tee "\$INIT_LOG"
    echo ">> Dropping cct_tpcc_drop_old if it exists"
    ./cockroach sql --url "${PG_URL_N1}" -e "DROP DATABASE cct_tpcc_drop_old CASCADE;"
    sleep 5
    echo ">> Starting tpcc workload for 1h"
    ./cockroach workload run tpcc \
        --warehouses 3000 \
        --active-warehouses 1000 \
        --db cct_tpcc_drop \
        --secure \
  --prometheus-port 2113 \
        --ramp 5m \
        --display-every 5s \
        --duration 60m \
  --tolerate-errors \
   "\${PGURLS_ARR[@]}" | tee "\$RUN_LOG"

    echo ">> Renaming to cct_tpcc_drop_old"
    ./cockroach sql --url "${PG_URL_N1}" -e "ALTER DATABASE cct_tpcc_drop RENAME TO cct_tpcc_drop_old;"

    sleep 1
done
EOF

  # Upload the script to the workload cluster
  drtprod put $WORKLOAD_CLUSTER:$NODE /tmp/tpcc_drop.sh
  drtprod ssh $WORKLOAD_CLUSTER:$NODE -- "chmod +x tpcc_drop.sh"
  if [ "$execute_script" = "true" ]; then
    drtprod run "${WORKLOAD_CLUSTER}":1 -- "sudo systemd-run --unit tpcc_drop --same-dir --uid \$(id -u) --gid \$(id -g) bash ${pwd}/tpcc_drop.sh"
  fi
done
