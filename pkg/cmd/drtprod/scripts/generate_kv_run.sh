#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# This script sets up the kv workload script in the workload nodes
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

PGURLS=$(drtprod pgurl $CLUSTER --external | sed s/\'//g)

# Loop through each node
for NODE in $(seq 1 $WORKLOAD_NODES)
do
  # Create the workload script
  cat <<EOF >/tmp/kv_run.sh
#!/usr/bin/env bash

read -r -a PGURLS_ARR <<< "$PGURLS"

j=0
while true; do
    echo ">> Starting kv workload"
    ((j++))
    LOG=./kv_$j.txt
    ./cockroach workload run kv \
        --init \
        --drop \
        --concurrency 128 \
        --histograms kv/stats.json \
        --db kv \
        --splits 1000 \
        --read-percent 50 \
        --span-percent 20 \
        --cycle-length 100000 \
        --min-block-bytes 100 \
        --max-block-bytes 1000 \
        --prometheus-port 2114 \
        --max-rate 200 \
        --secure \
        --ramp 10m \
        --display-every 5s \
        --duration 12h \
        --tolerate-errors \
        --enum \
        "\${PGURLS_ARR[@]}" | tee "\$LOG"
    if [ \$? -eq 0 ]; then
        rm "\$LOG"
    fi
    sleep 1
done
EOF

  # Upload the script to the workload cluster
  drtprod put $WORKLOAD_CLUSTER:$NODE /tmp/kv_run.sh
  drtprod ssh $WORKLOAD_CLUSTER:$NODE -- "chmod +x kv_run.sh"
  if [ "$execute_script" = "true" ]; then
    drtprod run "${WORKLOAD_CLUSTER}":1 -- "sudo systemd-run --unit kv_run --same-dir --uid \$(id -u) --gid \$(id -g) bash ${pwd}/kv_run.sh"
  fi
done
