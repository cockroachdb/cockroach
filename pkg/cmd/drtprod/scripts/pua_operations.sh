#!/bin/bash
# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# This script sets up operation-specific execution scripts on a workload cluster.
# It verifies required environment variables, retrieves Datadog API credentials,
# generates operation scripts for predefined scenarios (disk stall, full network partition,
# partial network partition), and deploys these scripts to the workload cluster nodes.

# Check required environment variables
check_env_vars() {
    local required_vars=("CLUSTER" "WORKLOAD_CLUSTER")
    for var in "${required_vars[@]}"; do
        if [ -z "${!var}" ]; then
            echo "environment ${var} is not set"
            exit 1
        fi
    done
}

# Get Datadog API key
get_dd_api_key() {
    if [ -z "${DD_API_KEY}" ]; then
        DD_API_KEY="$(gcloud --project=cockroach-drt secrets versions access latest --secret datadog-api-key)"
        if [ -z "${DD_API_KEY}" ]; then
            echo "Missing Datadog API key!"
            exit 1
        fi
    fi
}

# Sanitize operation name for filename
sanitize_filename() {
    local operation=$1
    echo "${operation//\//-}"  # Replace / with -
}

# Create operation script
create_operation_script() {
    local operation=$1
    local safe_name=$(sanitize_filename "$operation")
    local filename="run_ops_${safe_name}.sh"
    local iterations=${2:-1}  # Default to 1 if not specified
    local wait_time=${3:-"10s"}  # Default wait time
    
    local script_content="#!/bin/bash

export ROACHPROD_GCE_DEFAULT_PROJECT=${ROACHPROD_GCE_DEFAULT_PROJECT}
export ROACHPROD_DNS=${ROACHPROD_DNS}
"

    if [ "$iterations" -gt 1 ]; then
        script_content+="
for i in {1..$iterations}; do
    echo \"Running ${operation} iteration \$i/$iterations...\"
"
    fi

    script_content+="
    /home/ubuntu/roachtest run-operation ${CLUSTER} \"${operation}\" --datadog-api-key ${DD_API_KEY} \
    --datadog-tags env:development,cluster:${WORKLOAD_CLUSTER},team:drt,service:drt-cockroachdb \
    --datadog-app-key 1 --certs-dir ./certs --wait-before-cleanup ${wait_time} | tee -a roachtest_ops_${safe_name}.log
"

    if [ "$iterations" -gt 1 ]; then
        script_content+="
    SLEEP_TIME=\$((20 + RANDOM % 11))
    echo \"Sleeping for \$SLEEP_TIME seconds before next iteration...\"
    sleep \$SLEEP_TIME
done"
    fi

    drtprod ssh "${WORKLOAD_CLUSTER}":1 -- "tee /home/ubuntu/${filename} > /dev/null << 'EOF'
${script_content}
EOF"

    drtprod ssh "${WORKLOAD_CLUSTER}":1 -- chmod +x /home/ubuntu/"${filename}"
}

# Get operation configuration
get_operation_config() {
    local op=$1
    case "$op" in
        "disk-stall")
            echo "disk-stall,50,10s"
            ;;
        "network-partition-full")
            echo "network-partition/full,1,5m"
            ;;
        "network-partition-partial")
            echo "network-partition/partial,1,5m"
            ;;
        *)
            echo ""
            ;;
    esac
}

# List available operations
list_operations() {
    echo "Available operations:"
    echo "  - disk-stall"
    echo "  - network-partition-full"
    echo "  - network-partition-partial"
}

# Main execution
main() {
    check_env_vars
    get_dd_api_key

    # the ssh keys of all workload nodes should be setup on the crdb nodes for the operations
    drtprod ssh ${CLUSTER} -- "echo \"$(drtprod run "${WORKLOAD_CLUSTER}":1 -- cat ./.ssh/id_rsa.pub|grep ssh-rsa)\" >> ./.ssh/authorized_keys"

    local operations=("disk-stall" "network-partition-full" "network-partition-partial")
    
    for opname in "${operations[@]}"; do
        echo "Setting up script for operation: ${opname}"
        local config=$(get_operation_config "$opname")
        IFS=',' read -r op_name op_iterations op_wait <<< "$config"
        create_operation_script "$op_name" "$op_iterations" "$op_wait"
        echo "Script for operation: ${opname} complete"
        echo "----------------------------------------"
    done
}

main "$@"
