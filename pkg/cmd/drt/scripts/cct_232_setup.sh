#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -eux

CLUSTER=cct-232
USERNAME=cct
NUM_NODES=${NUM_NODES:-15}
CRDB_NODES=1-$((NUM_NODES-1))
WORKLOAD_NODE=$NUM_NODES
MACHINE_TYPE=n2-standard-8
VOLUME_SIZE=1000 # 1 TB
LIFETIME=2160h   # 90 days
ZONES=us-east1-b,us-east1-c,us-east1-d
SKIP_BUILD=${SKIP_BUILD:-false} # test-only

# We bootstrap the cluster in the previous version to test upgrades
# and mixed-version states.
INITIAL_VERSION=v23.1.11

#### CLUSTER SETUP START ####

# Build roachprod to make sure we have the latest version.
[ $SKIP_BUILD == 'true' ] || ./dev build roachprod

# Build cockroach.
[ $SKIP_BUILD == 'true' ] || ./dev build cockroach --cross

# Create the cluster itself.
./bin/roachprod create ${CLUSTER} \
                --clouds gce \
                --nodes ${NUM_NODES} \
                --gce-machine-type ${MACHINE_TYPE} \
                --gce-pd-volume-size ${VOLUME_SIZE} \
                --local-ssd=false \
                --gce-zones ${ZONES} \
                --label "usage=cct-232" \
                --username $USERNAME \
                --lifetime $LIFETIME

# Stage the initial version binary in the cockroach nodes.
./bin/roachprod run $CLUSTER:$CRDB_NODES mkdir $INITIAL_VERSION
./bin/roachprod stage $CLUSTER:$CRDB_NODES release $INITIAL_VERSION --dir $INITIAL_VERSION

# Start the cluster
./bin/roachprod start $CLUSTER:$CRDB_NODES \
                --binary $INITIAL_VERSION/cockroach \
                --args "--config-profile=replication-source" \
                --schedule-backups \
                --secure

# Use the most recent `cockroach` binary for the workload node.
./bin/roachprod put $CLUSTER:$WORKLOAD_NODE ./artifacts/cockroach ./cockroach

# Enable node_exporter on all nodes
./bin/roachprod grafana-start $CLUSTER

#### CLUSTER SETUP END ####

#### TPCC WORKLOAD START ####

PGURLS=$(./bin/roachprod pgurl --secure $CLUSTER:$CRDB_NODES)

TPCC_WAREHOUSES=5000

TPCC_DB=cct_tpcc
TPCC_USER="cct_tpcc_user"
TPCC_PASSWORD="tpcc"

cat <<EOF >/tmp/tpcc_init.sh
#!/usr/bin/env bash
./cockroach workload init tpcc \
    --warehouses $TPCC_WAREHOUSES \
    --db $TPCC_DB \
    --secure \
    --families \
    $PGURLS
EOF

./bin/roachprod put $CLUSTER:$WORKLOAD_NODE /tmp/tpcc_init.sh tpcc_init.sh
./bin/roachprod run $CLUSTER:$WORKLOAD_NODE chmod +x tpcc_init.sh

cat <<EOF >/tmp/tpcc_run.sh
#!/usr/bin/env bash
set -o pipefail
j=0
while true; do
    echo ">> Starting tpcc workload"
    ((j++))
    LOG=./tpcc_\$j.txt
    ./cockroach workload run tpcc \
        --warehouses $TPCC_WAREHOUSES \
        --scatter \
        --max-rate 2000 \
        --db $TPCC_DB \
        --secure \
        --ramp 10m \
        --display-every 5s \
        --duration 12h \
        --user $TPCC_USER \
        --password $TPCC_PASSWORD \
        --families \
        $PGURLS | tee "\$LOG"
    if [ $? -eq 0 ]; then
        rm "\$LOG"
    fi
    sleep 1
done
EOF

./bin/roachprod put $CLUSTER:$WORKLOAD_NODE /tmp/tpcc_run.sh tpcc_run.sh
./bin/roachprod run $CLUSTER:$WORKLOAD_NODE chmod +x tpcc_run.sh

# Initialize workload, create user that will run it, then run it.
./bin/roachprod run $CLUSTER:$WORKLOAD_NODE ./tpcc_init.sh

./bin/roachprod sql $CLUSTER:1 --secure --binary $INITIAL_VERSION/cockroach -- -e "CREATE USER $TPCC_USER WITH LOGIN PASSWORD '$TPCC_PASSWORD'; GRANT ALL PRIVILEGES ON DATABASE $TPCC_DB TO $TPCC_USER;"
./bin/roachprod run $CLUSTER:$WORKLOAD_NODE -- sudo systemd-run --working-directory=/home/ubuntu --service-type exec --collect --unit cct_tpcc ./tpcc_run.sh

#### TPCC WORKLOAD END ####

#### KV WORKLOAD START ####

KV_DB=cct_kv

# TODO: run with user/password (non-root)

KV_PERFDIR=/mnt/data1/kv_perf

./bin/roachprod run $CLUSTER:$WORKLOAD_NODE -- mkdir -p $KV_PERFDIR

cat <<EOF >/tmp/kv_run.sh
#!/usr/bin/env bash
set -o pipefail
j=0
while true; do
    echo ">> Starting kv workload"
    ((j++))
    LOG=./kv_\$j.txt
    ./cockroach workload run kv \
        --init \
        --drop \
        --concurrency 128 \
        --histograms ${KV_PERFDIR}/stats.json \
        --db $KV_DB \
        --splits 1000 \
        --read-percent 50 \
        --span-percent 20 \
        --cycle-length 100000 \
        --min-block-bytes 100 \
        --max-block-bytes 1000 \
        --max-rate 1200 \
        --secure \
        --ramp 10m \
        --display-every 5s \
        --duration 12h \
        --enum \
        $PGURLS | tee "\$LOG"
    if [ $? -eq 0 ]; then
        rm "\$LOG"
    fi
    sleep 1
done
EOF

./bin/roachprod put $CLUSTER:$WORKLOAD_NODE /tmp/kv_run.sh kv_run.sh
./bin/roachprod run $CLUSTER:$WORKLOAD_NODE chmod +x kv_run.sh

# Start workload.
./bin/roachprod run $CLUSTER:$WORKLOAD_NODE -- sudo systemd-run --working-directory=/home/ubuntu --service-type exec --collect --unit cct_kv ./kv_run.sh

#### KV WORKLOAD END ####
