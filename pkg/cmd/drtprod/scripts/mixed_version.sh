#!/bin/bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# This script schedules daily maintenance commands on a 2‐week cycle.
# Optional parameters OLD_RELEASE and NEW_RELEASE can be provided.
# Usage: ./mixed_version.sh <CLUSTER> [OLD_RELEASE=v24.3.8] [NEW_RELEASE=v25.2.0-alpha.2]

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

CLUSTER="$1"
if [ -z "$CLUSTER" ]; then
    echo "Usage: $0 <CLUSTER> [OLD_RELEASE] [NEW_RELEASE]"
    exit 1
fi

export ROACHPROD_DISABLED_PROVIDERS=IBM

/home/ubuntu/drtprod sync

# Set optional release versions
OLD_RELEASE="${2:-v24.3.8}"
NEW_RELEASE="${3:-v25.2.0-alpha.2}"

# Get today's day of week (1 for Monday, ... 7 for Sunday) and date
day_of_week=$(date +%u)
today=$(date +%F)
cycle_file="/home/ubuntu/.cycle_info.txt"

if [ -f "$cycle_file" ]; then
    read saved_cycle saved_day < "$cycle_file"
else
    # Initialize cycle_week to 0 if no previous info exists
    saved_cycle=0
    saved_day=$today
fi

# On Monday, if this is the first run of today, flip the cycle week
if [ "$day_of_week" -eq 1 ] && [ "$saved_day" != "$today" ]; then
    cycle_week=$((1 - saved_cycle))
else
    cycle_week=$saved_cycle
fi

# Save the cycle week and today's date for persistence
echo "$cycle_week $today" > "$cycle_file"

# Use an array to store multiple commands
cmds=()

if [ "$day_of_week" -eq 1 ] && [ "$cycle_week" -eq 1 ]; then
        # Week 2 - Monday
        cmds+=("/home/ubuntu/drtprod deploy $CLUSTER:3-5 release $NEW_RELEASE")
elif [ "$day_of_week" -eq 2 ] && [ "$cycle_week" -eq 0 ]; then
        # Tuesday in Week 1 only
        cmds+=("sudo systemctl stop tpcc_run_cct_tpcc")
        cmds+=("sudo systemctl stop roachtest_ops")
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
        # Note that roachtest_operations_run.sh needs to be setup manually for the first time.
        cmds+=("sudo systemd-run --unit roachtest_ops --same-dir --uid $(id -u) --gid $(id -g) bash /home/ubuntu/roachtest_operations_run.sh")
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

if [ ${#cmds[@]} -gt 0 ]; then
    for cmd in "${cmds[@]}"; do
        echo "Executing: $cmd"
        if ! eval "$cmd"; then
            echo "Error executing: $cmd" >&2
            exit 1
        fi
    done
else
    echo "No scheduled command for today."
fi
