#!/bin/bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# This script schedules daily maintenance commands on a 2‐week cycle.
# Optional parameters OLD_RELEASE and NEW_RELEASE can be provided.
# Usage: ./mixed_version.sh <CLUSTER> <WORKLOAD_CLUSTER> <CLOUD> [OLD_RELEASE] [NEW_RELEASE] [DAY_OVERRIDE]
# DAY_OVERRIDE: optionally force a specific day's action (monday, tuesday, thursday, friday)

# Schedule of commands:
#
# Week 1:
# - Tuesday:
#     - Wipe the cluster
#     - Full cluster restart with OLD_RELEASE
#     - Set cluster.preserve_downgrade_option to OLD_RELEASE
#     - upgrade nodes 1-3 to NEW_RELEASE (50% nodes)
#     - Start tpcc_init_cct_tpcc.sh and roachtest_operations_run.sh
# - Thursday:
#     - Deploy NEW_RELEASE to the entire cluster (not finalized)
# - Friday:
#     - Revert all nodes to OLD_RELEASE
#     - upgrade nodes 1-2 to NEW_RELEASE (33% nodes)
#
# Week 2:
# - Monday:
#     - Upgrade nodes 3-5 to NEW_RELEASE (80% nodes as 1,2 were already upgraded)
# - Friday:
#     - Reset cluster.preserve_downgrade_option
#     - Upgrade node 6 to NEW_RELEASE (100% nodes as 1-5 were already upgraded)

USAGE="Usage: $0 <CLUSTER> <WORKLOAD_CLUSTER> <CLOUD> [OLD_RELEASE] [NEW_RELEASE] [DAY_OVERRIDE]"

CLUSTER="$1"
WORKLOAD_CLUSTER="$2"
CLOUD="$3"
if [ -z "$CLUSTER" ] || [ -z "$WORKLOAD_CLUSTER" ] || [ -z "$CLOUD" ]; then
    echo "$USAGE"
    exit 1
fi

export ROACHPROD_DISABLED_PROVIDERS=IBM

/home/ubuntu/drtprod sync

# Set optional release versions
OLD_RELEASE="${4:-v25.4.6}"
NEW_RELEASE="${5:-v26.2.0-beta.1}"

# Get today's day of week (1 for Monday, ... 7 for Sunday) and date
# An optional 6th argument overrides the day for manual triggering.
DAY_OVERRIDE="${6:-}"
case "$DAY_OVERRIDE" in
    monday)    day_of_week=1 ;;
    tuesday)   day_of_week=2 ;;
    wednesday) day_of_week=3 ;;
    thursday)  day_of_week=4 ;;
    friday)    day_of_week=5 ;;
    "")        day_of_week=$(date +%u) ;;
    *)         echo "Invalid day override: $DAY_OVERRIDE (use monday, tuesday, thursday, or friday)"; exit 1 ;;
esac
today=$(date +%F)
cycle_file="/home/ubuntu/.cycle_info.txt"

first_run=false
if [ -f "$cycle_file" ]; then
    read saved_cycle saved_day < "$cycle_file"
else
    # Initialize cycle_week to 0 if no previous info exists
    saved_cycle=0
    saved_day=$today
    first_run=true
fi

# On Monday, if this is the first run of today, flip the cycle week.
# Use the real day of week here so that a day override doesn't accidentally
# flip the cycle.
actual_day_of_week=$(date +%u)
if [ "$actual_day_of_week" -eq 1 ] && [ "$saved_day" != "$today" ]; then
    cycle_week=$((1 - saved_cycle))
else
    cycle_week=$saved_cycle
fi

# Save the cycle week and today's date for persistence
echo "$cycle_week $today" > "$cycle_file"

# Use an array to store multiple commands
cmds=()

# Appends the cluster initialization commands (wipe, stage, start, upgrade, workloads).
# Used on Week 1 Tuesday and also on first-ever run regardless of day.
append_init_cmds() {
        cmds+=("sudo systemctl stop tpcc_run_cct_tpcc || true")
        cmds+=("sudo systemctl reset-failed tpcc_run_cct_tpcc || true")
        cmds+=("sudo systemctl stop roachtest_ops || true")
        cmds+=("sudo systemctl reset-failed roachtest_ops || true")
        cmds+=("/home/ubuntu/drtprod stop $CLUSTER")
        cmds+=("/home/ubuntu/drtprod wipe $CLUSTER")
        cmds+=("/home/ubuntu/drtprod stage $CLUSTER release $OLD_RELEASE")
        cmds+=("/home/ubuntu/drtprod start $CLUSTER --binary ./cockroach --args=--wal-failover=among-stores --enable-fluent-sink=true --restart=false --sql-port=26257 --store-count=4")
        version=$(echo "$OLD_RELEASE" | sed -E 's/^v([0-9]+\.[0-9]+)\..*/\1/')
        cmds+=("/home/ubuntu/drtprod sql $CLUSTER:1 -- -e \"SET CLUSTER SETTING cluster.preserve_downgrade_option ='$version'\"")
        cmds+=("/home/ubuntu/drtprod deploy $CLUSTER:1-3 release $NEW_RELEASE")
        cmds+=("rm -rf /home/ubuntu/certs")
        cmds+=("/home/ubuntu/drtprod get $CLUSTER:1 certs /home/ubuntu/certs")
        cmds+=("chmod 600 /home/ubuntu/certs/*")
        cmds+=("/home/ubuntu/tpcc_init_cct_tpcc.sh")
        cmds+=("sudo systemd-run --unit tpcc_run_cct_tpcc --same-dir --uid $(id -u) --gid $(id -g) bash /home/ubuntu/tpcc_run_cct_tpcc.sh")
        cmds+=("sleep 30")
        # TODO: parameterize
        cmds+=("sudo systemd-run --unit roachtest_ops --same-dir --uid $(id -u) --gid $(id -g) bash /home/ubuntu/roachtest_operations_run.sh $CLUSTER $WORKLOAD_CLUSTER $CLOUD")
}

if [ "$first_run" = true ]; then
        # First run ever: initialize the cluster regardless of day of week
        echo "First run detected; running cluster initialization."
        append_init_cmds
elif [ "$day_of_week" -eq 1 ] && [ "$cycle_week" -eq 1 ]; then
        # Week 2 - Monday
        cmds+=("/home/ubuntu/drtprod deploy $CLUSTER:3-5 release $NEW_RELEASE")
elif [ "$day_of_week" -eq 2 ] && [ "$cycle_week" -eq 0 ]; then
        # Tuesday in Week 1 only
        append_init_cmds
elif [ "$day_of_week" -eq 4 ] && [ "$cycle_week" -eq 0 ]; then
    # Thursday in Week 1 only
    cmds+=("/home/ubuntu/drtprod deploy $CLUSTER:4-6 release $NEW_RELEASE")
elif [ "$day_of_week" -eq 5 ]; then
    # Friday for both Weeks
    if [ "$cycle_week" -eq 0 ]; then
        # Week 1 friday commands
        cmds+=("/home/ubuntu/drtprod deploy $CLUSTER release $OLD_RELEASE")
        cmds+=("/home/ubuntu/drtprod deploy $CLUSTER:1-2 release $NEW_RELEASE")
    else
        # Week 2 friday commands
        cmds+=("/home/ubuntu/drtprod sql $CLUSTER:1 -- -e 'RESET CLUSTER SETTING cluster.preserve_downgrade_option'")
        cmds+=("/home/ubuntu/drtprod deploy $CLUSTER:6 release $NEW_RELEASE")
    fi
fi

# Always check the status of the cluster
cmds+=("/home/ubuntu/drtprod status $CLUSTER")

for cmd in "${cmds[@]}"; do
    echo "Executing: $cmd"
    if ! eval "$cmd"; then
        echo "Error executing: $cmd" >&2
        exit 1
    fi
done
