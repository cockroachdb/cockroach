#!/usr/bin/env bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# This script sets up the gitload workload script on the workload node.
# It generates a synthetic git repo, initializes the schema, and runs
# continuous ingestion cycles with varying topological orderings.
#
# NOTE - This uses CLUSTER and WORKLOAD_CLUSTER environment variables.
# If not set the script fails.
#
# The workload node must have git installed.
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

export ROACHPROD_DISABLED_PROVIDERS=IBM

PGURLS=$(drtprod pgurl $CLUSTER --external | sed s/\'//g)

# Create the workload script
cat <<EOF >/tmp/gitload_run.sh
#!/usr/bin/env bash

read -r -a PGURLS_ARR <<< "$PGURLS"

j=0
while true; do
    echo ">> Starting gitload workload (iteration \$j)"
    ((j++))
    LOG=./gitload_\$j.txt

    # Init: creates tables, generates synthetic repos (one per agent),
    # ingests commits, and adds FK constraints.
    # Safe to re-run: reuses existing repos if they have enough commits.
    ./cockroach workload init gitload \
        --commits 1000 \
        --seed 42 \
        --max-blob-size 65536 \
        --batch-size 50 \
        --concurrency 4 \
        --repo /tmp/gitload-repo \
        --secure \
        "\${PGURLS_ARR[@]}" 2>&1 | tee -a "\$LOG"

    # Run: continuous clear -> re-ingest cycles with rotating seeds.
    # Each agent clears its data and re-ingests with a new topological
    # ordering, creating sustained write load with FK/index contention.
    ./cockroach workload run gitload \
        --commits 1000 \
        --seed 42 \
        --max-blob-size 65536 \
        --batch-size 50 \
        --concurrency 4 \
        --inline-verify 300 \
        --repo /tmp/gitload-repo \
        --histograms gitload/stats.json \
        --prometheus-port 2115 \
        --display-every 5s \
        --duration 12h \
        --tolerate-errors \
        --secure \
        "\${PGURLS_ARR[@]}" 2>&1 | tee -a "\$LOG"
    if [ \$? -eq 0 ]; then
        rm "\$LOG"
    fi
    sleep 1
done
EOF

# Upload the script to the workload cluster
drtprod put $WORKLOAD_CLUSTER:1 /tmp/gitload_run.sh
drtprod ssh $WORKLOAD_CLUSTER:1 -- "chmod +x gitload_run.sh"
if [ "$execute_script" = "true" ]; then
  drtprod run "${WORKLOAD_CLUSTER}":1 -- "sudo systemd-run --unit gitload_run --same-dir --uid \$(id -u) --gid \$(id -g) bash \${PWD}/gitload_run.sh"
fi
