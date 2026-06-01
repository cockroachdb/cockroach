#!/usr/bin/env bash
#
# Test changefeeds running in a cluster with very many tables.
#
# Two modes:
#   --setup <num_tables>   Create cluster, init schema, load data, then exit.
#   <num_changefeeds> ...  Run changefeeds on an existing cluster.
#
# Usage:
#   bash test_changefeed_many_tables.sh --setup <num_tables>
#   bash test_changefeed_many_tables.sh <num_changefeeds> <sink> [tables_per_cf] [duration] [workers]
#
# Examples:
#   bash test_changefeed_many_tables.sh --setup 10000
#   bash test_changefeed_many_tables.sh --setup 100000
#   bash test_changefeed_many_tables.sh 1000 webhook 1 30m 16
#   bash test_changefeed_many_tables.sh 10 webhook-slow 100 1h 32
#   bash test_changefeed_many_tables.sh 5 cloud

set -euo pipefail

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CLUSTER_PREFIX="keithchow-proj"
WAREHOUSES=1
CRDB_NODES="1-9"
WL_NODE="10"
WEBHOOK_PORT=9707
LIFETIME="720h"
TPCC_TABLES=(warehouse district customer history "order" new_order item stock order_line)
CHANGEFEED_OPTS_WEBHOOK="format=json, updated, resolved='10s', webhook_sink_config='{\"Flush\":{\"Messages\":100,\"Frequency\":\"5s\"},\"Retry\":{\"Max\":\"inf\",\"Backoff\":\"500ms\"}}'"
CHANGEFEED_OPTS_WEBHOOK_SLOW="format=json, updated, resolved='10s', webhook_sink_config='{\"Flush\":{\"Messages\":100,\"Frequency\":\"5s\"},\"Retry\":{\"Max\":\"inf\",\"Backoff\":\"500ms\"}}'"
CHANGEFEED_OPTS_CLOUD="format=json, updated, resolved='10s'"
ROACHPROD="${ROACHPROD:-./bin/roachprod}"

# Ensure roachprod is available.
if [[ ! -f "$ROACHPROD" ]]; then
    ROACHPROD=$(which roachprod 2>/dev/null || true)
    if [[ -z "$ROACHPROD" ]]; then
        echo "ERROR: roachprod not found. Build it with: ./dev build roachprod"
        exit 1
    fi
fi

# Helper to time phases.
declare -A PHASE_TIMES
phase_start() {
    PHASE_START=$SECONDS
}
phase_end() {
    local duration=$(( SECONDS - PHASE_START ))
    PHASE_TIMES["$CURRENT_PHASE"]=$duration
    echo "  (phase took ${duration}s)"
}

# Cleanup on exit.
TMPFILES=()
cleanup_tmp() {
    for f in "${TMPFILES[@]}"; do
        rm -rf "$f" 2>/dev/null || true
    done
}
trap cleanup_tmp EXIT

# ---------------------------------------------------------------------------
# Helper: generate db-list file for a given num_tables
# ---------------------------------------------------------------------------
generate_db_list() {
    local num_tables=$1
    local out_file=$2
    local remaining=$num_tables
    local db_idx=0
    local num_schemas=1
    local max_schemas=72
    local tables_per_schema=9

    > "$out_file"
    while (( remaining > 0 )); do
        local db_name="warehouse_${db_idx}"
        for (( s=0; s < num_schemas && remaining > 0; s++ )); do
            if (( s == 0 )); then
                echo "${db_name}.public" >> "$out_file"
            else
                echo "${db_name}.schema_${s}" >> "$out_file"
            fi
            remaining=$(( remaining - tables_per_schema ))
        done
        num_schemas=$(( num_schemas + 1 > max_schemas ? max_schemas : num_schemas + 1 ))
        db_idx=$(( db_idx + 1 ))
    done
}

# ---------------------------------------------------------------------------
# Helper: derive cluster name from table count
# ---------------------------------------------------------------------------
cluster_name() {
    local num_tables=$1
    if (( num_tables >= 1000000 )); then
        echo "${CLUSTER_PREFIX}-$((num_tables / 1000000))m-tables"
    elif (( num_tables >= 1000 )); then
        echo "${CLUSTER_PREFIX}-$((num_tables / 1000))k-tables"
    else
        echo "${CLUSTER_PREFIX}-${num_tables}-tables"
    fi
}

# ---------------------------------------------------------------------------
# Helper: detect existing cluster
# ---------------------------------------------------------------------------
detect_cluster() {
    local cluster
    cluster=$($ROACHPROD list --json 2>/dev/null | python3 -c "
import sys, json
data = json.load(sys.stdin)
for name in sorted(data.get('clusters', {}).keys()):
    if name.startswith('${CLUSTER_PREFIX}-') and name.endswith('-tables'):
        print(name)
        break
" 2>/dev/null || true)
    if [[ -z "$cluster" ]]; then
        echo "ERROR: No ${CLUSTER_PREFIX}-*-tables cluster found. Run --setup first." >&2
        exit 1
    fi
    echo "$cluster"
}

# ===========================================================================
# --cancel mode: cancel all changefeed jobs, then exit
# ===========================================================================
if [[ "${1:-}" == "--cancel" ]]; then
    CLUSTER=$(detect_cluster)
    echo "=== Cancelling changefeed jobs on $CLUSTER ==="

    echo "  Job summary:"
    $ROACHPROD sql "$CLUSTER:1" -- -e \
        "SELECT status, count(*) FROM system.jobs WHERE job_type = 'CHANGEFEED' GROUP BY status;" \
        2>&1 || true

    echo ""
    echo "  Cancelling (batches of 100)..."
    cancelled=0
    max_iterations=1000
    for (( i=0; i < max_iterations; i++ )); do
        result=$($ROACHPROD sql "$CLUSTER:1" -- -e \
            "CANCEL JOBS SELECT id FROM system.jobs WHERE job_type = 'CHANGEFEED' AND status = 'running' LIMIT 100;" \
            2>&1 || true)
        if echo "$result" | grep -q "CANCEL JOBS 0"; then
            break
        fi
        cancelled=$((cancelled + 100))
        if (( cancelled % 1000 == 0 )); then
            echo "  Cancelled $cancelled jobs..."
        fi
    done
    echo "  Done. Cancelled ~$cancelled jobs."
    exit 0
fi

# ===========================================================================
# --setup mode: create cluster and load data, then exit
# ===========================================================================
if [[ "${1:-}" == "--setup" ]]; then
    shift
    # Parse optional --no-fks flag.
    NO_FKS=false
    if [[ "${1:-}" == "--no-fks" ]]; then
        NO_FKS=true
        shift
    fi
    if (( $# < 1 )); then
        echo "Usage: $0 --setup [--no-fks] <num_tables>"
        exit 1
    fi

    NUM_TABLES="$1"
    CLUSTER=$(cluster_name "$NUM_TABLES")

    echo "=== Setup: $NUM_TABLES tables ==="
    echo "  Cluster: $CLUSTER"
    echo ""

    # --- Create cluster ---
    echo "--- Creating cluster ---"
    CURRENT_PHASE="setup_cluster"; phase_start

    if $ROACHPROD status "$CLUSTER" > /dev/null 2>&1; then
        echo "  Cluster $CLUSTER already exists, skipping creation."
    else
        echo "  Creating cluster $CLUSTER..."
        $ROACHPROD create "$CLUSTER" \
            --clouds gce \
            --nodes 10 \
            --gce-machine-type n2-standard-8 \
            --gce-pd-volume-size 800 \
            --gce-pd-volume-type pd-ssd \
            --local-ssd=false \
            --lifetime "$LIFETIME"

        echo "  Staging cockroach binary..."
        $ROACHPROD stage "$CLUSTER" cockroach

        echo "  Starting CRDB on nodes $CRDB_NODES..."
        $ROACHPROD start "$CLUSTER:$CRDB_NODES" --schedule-backups=false

        echo "  Cluster is up."
    fi

    phase_end

    # --- Apply cluster settings ---
    echo ""
    echo "--- Applying cluster settings ---"
    CURRENT_PHASE="setup_settings"; phase_start

    SETTINGS=(
        "SET CLUSTER SETTING sql.defaults.autocommit_before_ddl.enabled = 'false'"
        "SET CLUSTER SETTING sql.catalog.allow_leased_descriptors.enabled = 'true'"
        "SET CLUSTER SETTING sql.catalog.descriptor_lease.use_locked_timestamps.enabled = 'true'"
        "SET CLUSTER SETTING jobs.retention_time = '1h'"
        "SET CLUSTER SETTING kv.transaction.internal.max_auto_retries = 500"
        "SET CLUSTER SETTING sql.schema.approx_max_object_count = 0"
        "SET CLUSTER SETTING bulkio.import.row_count_validation.mode = 'off'"
        "SET CLUSTER SETTING kv.transaction.max_intents_bytes = 16777216"
        "SET CLUSTER SETTING kv.transaction.max_refresh_spans_bytes = 67108864"
        "SET CLUSTER SETTING kv.rangefeed.enabled = true"
    )
    for setting in "${SETTINGS[@]}"; do
        $ROACHPROD sql "$CLUSTER:1" -- -e "$setting"
    done

    phase_end

    # --- Init schema and load data ---
    echo ""
    echo "--- Initializing tpccmultidb schema ($NUM_TABLES tables) ---"
    CURRENT_PHASE="setup_init"; phase_start

    DB_LIST_FILE=$(mktemp /tmp/db_list.XXXXXX)
    TMPFILES+=("$DB_LIST_FILE")
    generate_db_list "$NUM_TABLES" "$DB_LIST_FILE"

    TOTAL_SCHEMAS=$(wc -l < "$DB_LIST_FILE" | tr -d ' ')
    echo "  $TOTAL_SCHEMAS database.schema entries"

    $ROACHPROD put "$CLUSTER:$WL_NODE" "$DB_LIST_FILE" db_list.txt

    PGURLS_QUOTED=$($ROACHPROD pgurl "$CLUSTER:$CRDB_NODES" --insecure)
    SQL_URL_QUOTED=$(echo "$PGURLS_QUOTED" | awk '{print $1}')

    mapfile -t ALL_SCHEMAS < "$DB_LIST_FILE"
    FIRST_DB="${ALL_SCHEMAS[0]%%.*}"
    if $ROACHPROD sql "$CLUSTER:1" -- -e "SELECT count(*) FROM ${FIRST_DB}.public.warehouse" 2>/dev/null | grep -q '[1-9]'; then
        echo "  Data already loaded, skipping init."
    else
        FKS_ARG=""
        if $NO_FKS; then
            FKS_ARG="--fks=false"
        fi
        echo "  Running workload init (this may take a while)..."
        $ROACHPROD run "$CLUSTER:$WL_NODE" -- \
            "./cockroach workload init tpccmultidb \
                --warehouses=$WAREHOUSES \
                --db-list-file=db_list.txt \
                $FKS_ARG \
                ${SQL_URL_QUOTED}"
        echo "  Init complete."
    fi

    phase_end

    echo ""
    echo "=== Setup Timing ==="
    for phase in $(echo "${!PHASE_TIMES[@]}" | tr ' ' '\n' | sort); do
        printf "  %-25s %ds\n" "$phase" "${PHASE_TIMES[$phase]}"
    done
    printf "  %-25s %ds\n" "TOTAL" "$SECONDS"

    echo ""
    echo "Setup done. Cluster: $CLUSTER"
    echo "  Run test:  $0 <num_changefeeds> <sink> [tables_per_cf] [duration] [workers]"
    echo "  Destroy:   $ROACHPROD destroy $CLUSTER"
    exit 0
fi

# ===========================================================================
# Run mode: create changefeeds and run workload on existing cluster
# ===========================================================================
if (( $# < 2 )); then
    echo "Usage: $0 --setup <num_tables>"
    echo "       $0 --cancel"
    echo "       $0 <num_changefeeds> <sink> [tables_per_cf] [duration] [workers]"
    echo ""
    echo "  --setup <num_tables>  Create cluster and load data"
    echo "  --cancel              Cancel all running changefeed jobs"
    echo "  num_changefeeds       Number of changefeeds to create"
    echo "  sink                  Sink type: webhook, webhook-slow, or cloud"
    echo "  tables_per_cf         Tables per changefeed (default: total_tables / num_changefeeds)"
    echo "  duration              Workload duration (default: 30m)"
    echo "  workers               Concurrent workers (default: 16)"
    exit 1
fi

NUM_CHANGEFEEDS="$1"
SINK_TYPE="$2"
TABLES_PER_CF="${3:-}"
DURATION="${4:-30m}"
WORKERS="${5:-16}"

# Validate sink type and select changefeed opts.
case "$SINK_TYPE" in
    webhook)
        CHANGEFEED_OPTS="$CHANGEFEED_OPTS_WEBHOOK"
        ;;
    webhook-slow)
        CHANGEFEED_OPTS="$CHANGEFEED_OPTS_WEBHOOK_SLOW"
        ;;
    cloud)
        CHANGEFEED_OPTS="$CHANGEFEED_OPTS_CLOUD"
        ;;
    *)
        echo "ERROR: Invalid sink type '$SINK_TYPE'. Must be: webhook, webhook-slow, or cloud."
        exit 1
        ;;
esac

CLUSTER=$(detect_cluster)

# Derive NUM_TABLES from the cluster name.
TABLE_PART="${CLUSTER#${CLUSTER_PREFIX}-}"
TABLE_PART="${TABLE_PART%-tables}"
if [[ "$TABLE_PART" == *m ]]; then
    NUM_TABLES=$(( ${TABLE_PART%m} * 1000000 ))
elif [[ "$TABLE_PART" == *k ]]; then
    NUM_TABLES=$(( ${TABLE_PART%k} * 1000 ))
else
    NUM_TABLES="$TABLE_PART"
fi

# Generate db-list.
DB_LIST_FILE=$(mktemp /tmp/db_list.XXXXXX)
TMPFILES+=("$DB_LIST_FILE")
generate_db_list "$NUM_TABLES" "$DB_LIST_FILE"

TOTAL_SCHEMAS=$(wc -l < "$DB_LIST_FILE" | tr -d ' ')
TOTAL_TABLES=$(( TOTAL_SCHEMAS * 9 ))
mapfile -t ALL_SCHEMAS < "$DB_LIST_FILE"

# Build flat table list.
ALL_FQ_TABLES=()
for dbschema in "${ALL_SCHEMAS[@]}"; do
    db="${dbschema%%.*}"
    schema="${dbschema#*.}"
    for tbl in "${TPCC_TABLES[@]}"; do
        ALL_FQ_TABLES+=("${db}.${schema}.${tbl}")
    done
done

# Compute tables per changefeed.
if [[ -z "$TABLES_PER_CF" ]]; then
    TABLES_PER_CF_ACTUAL=$(( (TOTAL_TABLES + NUM_CHANGEFEEDS - 1) / NUM_CHANGEFEEDS ))
else
    TABLES_PER_CF_ACTUAL=$TABLES_PER_CF
fi

# Upload db-list for the workload run.
$ROACHPROD put "$CLUSTER:$WL_NODE" "$DB_LIST_FILE" db_list.txt

# Get pgurls.
PGURLS_QUOTED=$($ROACHPROD pgurl "$CLUSTER:$CRDB_NODES" --insecure)

echo "=== Changefeed + many-tables test ==="
echo "  Cluster:          $CLUSTER"
echo "  CRDB nodes:       $CRDB_NODES"
echo "  Workload node:    $WL_NODE"
echo "  Total tables:     $TOTAL_TABLES ($TOTAL_SCHEMAS schemas)"
echo "  NUM_CHANGEFEEDS:  $NUM_CHANGEFEEDS"
echo "  TABLES_PER_CF:    $TABLES_PER_CF_ACTUAL"
echo "  SINK_TYPE:        $SINK_TYPE"
echo "  WAREHOUSES:       $WAREHOUSES"
echo "  DURATION:         $DURATION"
echo "  WORKERS:          $WORKERS"
echo ""

# ---------------------------------------------------------------------------
# Phase 1: Start the sink
# ---------------------------------------------------------------------------
echo "--- Phase 1: Starting $SINK_TYPE sink on node $WL_NODE ---"
CURRENT_PHASE="1_start_sink"; phase_start

if [[ "$SINK_TYPE" == "webhook" || "$SINK_TYPE" == "webhook-slow" ]]; then
    $ROACHPROD stop "$CLUSTER:$WL_NODE" 2>/dev/null || true
    sleep 1

    if [[ "$SINK_TYPE" == "webhook" ]]; then
        $ROACHPROD run "$CLUSTER:$WL_NODE" -- \
            "nohup ./cockroach workload debug webhook-server > webhook_server.log 2>&1 & echo \$!" || true
    else
        $ROACHPROD run "$CLUSTER:$WL_NODE" -- \
            "nohup ./cockroach workload debug webhook-server-slow 999999 100 100 100 100 100 100 100 100 100 100 > webhook_server.log 2>&1 & echo \$!" || true
    fi
    sleep 2

    WL_IP=$($ROACHPROD ip "$CLUSTER:$WL_NODE" 2>/dev/null | tr -d '[:space:]')
    if [[ -z "$WL_IP" ]]; then
        echo "ERROR: Could not get IP for workload node $WL_NODE."
        exit 1
    fi
    SINK_URL="webhook-https://${WL_IP}:${WEBHOOK_PORT}/?insecure_tls_skip_verify=true"
    echo "  Sink URL: $SINK_URL"

elif [[ "$SINK_TYPE" == "cloud" ]]; then
    CLOUD_TS=$(date +%Y%m%d%H%M%S)
    SINK_URL="experimental-gs://cockroach-tmp/roachtest/changefeed-many-tables/${CLOUD_TS}?AUTH=implicit"
    echo "  Sink URL: $SINK_URL"
fi

phase_end

# ---------------------------------------------------------------------------
# Phase 2: Create changefeeds
# ---------------------------------------------------------------------------
echo ""
echo "--- Phase 2: Creating $NUM_CHANGEFEEDS changefeed(s) ---"
CURRENT_PHASE="2_create_changefeeds"; phase_start

# Build a SQL file per node with all CREATE CHANGEFEED statements.
# Cache under /tmp keyed by parameters so repeated runs skip generation.
CF_CACHE_KEY="${NUM_TABLES}_${NUM_CHANGEFEEDS}_${TABLES_PER_CF_ACTUAL}"
CF_SQL_DIR="/tmp/cf_sql_cache_${CF_CACHE_KEY}"

if [[ -d "$CF_SQL_DIR" && -s "$CF_SQL_DIR/node_1.sql" ]]; then
    cf_total=$(cat "$CF_SQL_DIR"/node_*.sql | wc -l | tr -d ' ')
    echo "  Using cached SQL files ($cf_total statements in $CF_SQL_DIR)"
    # Update sink URL in cached files in case it changed.
    for (( n=1; n<=9; n++ )); do
        sed -i "s|INTO '[^']*'|INTO '${SINK_URL}'|g" "$CF_SQL_DIR/node_${n}.sql"
    done
else
    mkdir -p "$CF_SQL_DIR"
    for (( n=1; n<=9; n++ )); do
        > "$CF_SQL_DIR/node_${n}.sql"
    done

    offset=0
    cf_total=0
    for (( cf=0; cf < NUM_CHANGEFEEDS; cf++ )); do
        batch=("${ALL_FQ_TABLES[@]:$offset:$TABLES_PER_CF_ACTUAL}")
        (( ${#batch[@]} == 0 )) && break
        offset=$(( offset + TABLES_PER_CF_ACTUAL ))
        cf_total=$((cf_total + 1))

        table_list=$(IFS=', '; echo "${batch[*]}")
        node=$(( (cf % 9) + 1 ))

        echo "CREATE CHANGEFEED FOR TABLE ${table_list} INTO '${SINK_URL}' WITH ${CHANGEFEED_OPTS};" \
            >> "$CF_SQL_DIR/node_${node}.sql"

        if (( cf_total % 10000 == 0 )); then
            echo "  Prepared $cf_total/$NUM_CHANGEFEEDS statements..."
        fi
    done

    echo "  Generated $cf_total statements across 9 nodes."
fi

CF_CREATE_START=$($ROACHPROD sql "$CLUSTER:1" -- -e \
    "SELECT cluster_logical_timestamp()" 2>&1 | grep -oE '[0-9]{10,}\.[0-9]+' | head -1)

echo "  Executing in batches of 1000 per node..."

# Split each node's SQL file into chunks of 1000 and execute with retries.
CF_BATCH_DIR=$(mktemp -d /tmp/cf_batches.XXXXXX)
BATCH_SIZE=1000

for (( n=1; n<=9; n++ )); do
    sql_file="$CF_SQL_DIR/node_${n}.sql"
    if [[ -s "$sql_file" ]]; then
        mkdir -p "$CF_BATCH_DIR/node_${n}"
        split -l "$BATCH_SIZE" "$sql_file" "$CF_BATCH_DIR/node_${n}/batch_"
    fi
done

# Run all 9 nodes in parallel; each node runs its batches sequentially.
for (( n=1; n<=9; n++ )); do
    batch_dir="$CF_BATCH_DIR/node_${n}"
    if [[ -d "$batch_dir" ]]; then
        (
            for batch_file in "$batch_dir"/batch_*; do
                output=$($ROACHPROD sql "$CLUSTER:$n" -- < "$batch_file" 2>&1) || \
                    echo "  Node $n: batch $(basename $batch_file) failed: $(echo "$output" | grep -i 'error\|ERROR' | head -3)"
            done
        ) &
    fi
done

# Print progress while waiting for background jobs to finish.
while true; do
    # Check if any background jobs are still running.
    if ! jobs -r | grep -q .; then
        break
    fi
    status_summary=$($ROACHPROD sql "$CLUSTER:1" -- -e \
        "SELECT status, count(*) FROM system.jobs WHERE job_type = 'CHANGEFEED' AND status != 'canceled' AND created >= ${CF_CREATE_START}::DECIMAL GROUP BY status ORDER BY status" \
        2>&1 || echo "?")
    total=$($ROACHPROD sql "$CLUSTER:1" -- -e \
        "SELECT count(*) FROM system.jobs WHERE job_type = 'CHANGEFEED' AND status != 'canceled' AND created >= ${CF_CREATE_START}::DECIMAL" \
        2>&1 | grep -oE '[0-9]+' | tail -1 || echo "?")
    echo "  Changefeeds created: $total / $cf_total"
    echo "$status_summary" | sed 's/^/    /'
    sleep 30
done

wait
rm -rf "$CF_BATCH_DIR"

# Query actual count from the cluster.
cf_count=$($ROACHPROD sql "$CLUSTER:1" -- -e \
    "SELECT count(*) FROM system.jobs WHERE job_type = 'CHANGEFEED' AND status != 'canceled' AND created >= ${CF_CREATE_START}::DECIMAL" \
    2>&1 | grep -oE '[0-9]+' | tail -1 || echo "0")

echo "  Created $cf_count changefeed(s) (requested $cf_total)."
if (( cf_count < cf_total )); then
    echo "  WARNING: Only $cf_count out of $cf_total changefeeds were created."
fi

phase_end

# ---------------------------------------------------------------------------
# Phase 3: Run tpccmultidb workload on node 10
# ---------------------------------------------------------------------------
echo ""
echo "--- Phase 3: Running tpccmultidb workload for $DURATION ---"
CURRENT_PHASE="3_workload"; phase_start

FIRST_DB="${ALL_SCHEMAS[0]%%.*}"
$ROACHPROD run "$CLUSTER:$WL_NODE" -- \
    "./cockroach workload run tpccmultidb \
        --db=$FIRST_DB \
        --warehouses=$WAREHOUSES \
        --db-list-file=db_list.txt \
        --workers=$WORKERS \
        --wait=0.0 \
        --tolerate-errors \
        --duration=$DURATION \
        $PGURLS_QUOTED"

phase_end

# ---------------------------------------------------------------------------
# Phase 3b: SHOW CHANGEFEED JOBS (right after workload)
# ---------------------------------------------------------------------------
echo ""
echo "--- Phase 3b: SHOW CHANGEFEED JOBS (post-workload) ---"
CURRENT_PHASE="3b_show_changefeed_jobs"; phase_start
if ! $ROACHPROD sql "$CLUSTER:1" -- -e "SHOW CHANGEFEED JOBS;" > /dev/null 2>&1; then
    echo "  ERROR: SHOW CHANGEFEED JOBS failed."
    phase_end
    exit 1
fi
phase_end

# ---------------------------------------------------------------------------
# Phase 4: Wait for changefeeds to catch up
# ---------------------------------------------------------------------------
echo ""
echo "--- Phase 4: Waiting for changefeeds to catch up ---"
CURRENT_PHASE="4_catchup"; phase_start

WORKLOAD_END=$($ROACHPROD sql "$CLUSTER:1" -- -e \
    "SELECT cluster_logical_timestamp()" 2>&1 | grep -oE '[0-9]{10,}\.[0-9]+' | head -1)
echo "  Workload ended at: $WORKLOAD_END"

if [[ -z "$WORKLOAD_END" ]]; then
    echo "  WARNING: Could not get cluster timestamp, skipping catchup wait."
else
    POLL_INTERVAL=10
    elapsed=0
    caught_up=false
    while true; do
        # Count changefeeds that haven't reached the workload-end timestamp yet.
        # A NULL high_water_timestamp means initial scan hasn't finished.
        BEHIND=$($ROACHPROD sql "$CLUSTER:1" -- -e \
            "SELECT count(*) \
             FROM crdb_internal.jobs \
             WHERE job_type = 'CHANGEFEED' AND status = 'running' \
               AND created >= ${CF_CREATE_START}::DECIMAL \
               AND (high_water_timestamp IS NULL \
                    OR high_water_timestamp < ${WORKLOAD_END}::DECIMAL)" \
            2>&1 | grep -oE '[0-9]+' | tail -1 || echo "?")
        TOTAL_RUNNING=$($ROACHPROD sql "$CLUSTER:1" -- -e \
            "SELECT count(*) \
             FROM crdb_internal.jobs \
             WHERE job_type = 'CHANGEFEED' AND status = 'running' \
               AND created >= ${CF_CREATE_START}::DECIMAL" \
            2>&1 | grep -oE '[0-9]+' | tail -1 || echo "?")

        if [[ "$BEHIND" == "0" ]]; then
            echo "  All $TOTAL_RUNNING changefeeds caught up."
            echo "  Catchup time: ${elapsed}s"
            caught_up=true
            break
        fi
        echo "  $BEHIND / $TOTAL_RUNNING changefeeds still behind (polling ${elapsed}s)"
        sleep "$POLL_INTERVAL"
        elapsed=$(( elapsed + POLL_INTERVAL ))
    done

    if ! $caught_up; then
        echo "  WARNING: Catchup loop exited without catching up."
    fi
fi

phase_end

# ---------------------------------------------------------------------------
# Phase 5: SHOW CHANGEFEED JOBS
# ---------------------------------------------------------------------------
echo ""
echo "--- Phase 5: SHOW CHANGEFEED JOBS ---"
CURRENT_PHASE="5_show_changefeed_jobs"; phase_start
if ! $ROACHPROD sql "$CLUSTER:1" -- -e "SHOW CHANGEFEED JOBS;" > /dev/null 2>&1; then
    echo "  ERROR: SHOW CHANGEFEED JOBS failed."
    phase_end
    exit 1
fi
phase_end

# ---------------------------------------------------------------------------
# Phase 6: Summary and cleanup
# ---------------------------------------------------------------------------
echo ""
echo "--- Phase 6: Summary and cleanup ---"

if [[ "$SINK_TYPE" == "webhook" || "$SINK_TYPE" == "webhook-slow" ]]; then
    echo "  Webhook server stats:"
    $ROACHPROD run "$CLUSTER:$WL_NODE" -- \
        "echo -n '    Unique: ' && curl -sk https://localhost:${WEBHOOK_PORT}/unique && \
         echo -n '  Dupes: ' && curl -sk https://localhost:${WEBHOOK_PORT}/dupes && echo" || true
fi

echo ""
echo "  Changefeed job summary:"
CURRENT_PHASE="6a_job_summary"; phase_start
$ROACHPROD sql "$CLUSTER:1" -- -e \
    "SELECT status, count(*) FROM system.jobs WHERE job_type = 'CHANGEFEED' GROUP BY status;" \
    2>&1 || true
phase_end

echo ""
echo "  Cancelling changefeed jobs (batches of 100)..."
CURRENT_PHASE="6b_cancel_jobs"; phase_start
cancelled=0
max_iterations=1000
for (( i=0; i < max_iterations; i++ )); do
    result=$($ROACHPROD sql "$CLUSTER:1" -- -e \
        "CANCEL JOBS SELECT id FROM system.jobs WHERE job_type = 'CHANGEFEED' AND status = 'running' LIMIT 100;" \
        2>&1 || true)
    if echo "$result" | grep -q "CANCEL JOBS 0"; then
        break
    fi
    cancelled=$((cancelled + 100))
    if (( cancelled % 1000 == 0 )); then
        echo "  Cancelled $cancelled jobs..."
    fi
done
if (( i >= max_iterations )); then
    echo "  WARNING: Hit max iterations ($max_iterations) cancelling jobs."
fi
echo "  Done cancelling."
phase_end

echo ""
echo "=== Timing Summary ==="
for phase in $(echo "${!PHASE_TIMES[@]}" | tr ' ' '\n' | sort); do
    printf "  %-25s %ds\n" "$phase" "${PHASE_TIMES[$phase]}"
done
TOTAL_TIME=$SECONDS
printf "  %-25s %ds\n" "TOTAL" "$TOTAL_TIME"

echo ""
echo "Done. Cluster $CLUSTER is still running."
echo "  Destroy with: $ROACHPROD destroy $CLUSTER"
