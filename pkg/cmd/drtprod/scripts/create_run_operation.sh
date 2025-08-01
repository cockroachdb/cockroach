#!/bin/bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# This script sets up the tpcc import workload script in the workload node and starts the same in nohup
# NOTE - This uses CLUSTER and WORKLOAD_CLUSTER environment variable, if not set the script fails

# Check if at least one argument is provided
if [ "$#" -lt 1 ]; then
  echo "Usage: $0 \"identifier,operation_regex,cron_config\" \"identifier,operation_regex,cron_config\" ..."
  exit 1
fi

if [ -z "${CLUSTER}" ]; then
  echo "environment CLUSTER is not set"
  exit 1
fi

if [ -z "${WORKLOAD_CLUSTER}" ]; then
  echo "environment CLUSTER is not set"
  exit 1
fi
if [ -z "${DD_API_KEY}" ]; then
  DD_API_KEY="$(gcloud --project=cockroach-drt secrets versions access latest --secret datadog-api-key)"
fi

if [ -z "${DD_API_KEY}" ]; then
  echo "Missing Datadog API key!"
  exit 1
fi

# sync cluster is needed for operations
drtprod ssh ${WORKLOAD_CLUSTER} -- "ROACHPROD_GCE_DEFAULT_PROJECT=${ROACHPROD_GCE_DEFAULT_PROJECT} ./roachprod sync"

# the ssh keys of all workload nodes should be setup on the crdb nodes for the operations
drtprod ssh ${CLUSTER} -- "echo \"$(drtprod run ${WORKLOAD_CLUSTER} -- cat ./.ssh/id_rsa.pub|grep ssh-rsa)\" >> ./.ssh/authorized_keys"

absolute_path=$(drtprod run "${WORKLOAD_CLUSTER}":1 -- "realpath ./roachtest-operations")
pwd=$(drtprod run "${WORKLOAD_CLUSTER}":1 -- "dirname ${absolute_path}")
# Loop over all the passed arguments
for entry in "$@"; do
  # Split the entry into identifier, cron_config, and operation_regex using IFS and comma
  IFS=',' read -r identifier operation_regex cron_config <<< "$entry"

  # Check if all 3 parts are provided
  if [ -z "$identifier" ] || [ -z "$operation_regex" ]; then
    echo "Error: Each argument must have an identifier and operation_regex separated by commas."
    exit 1
  fi
  filename=run_ops_${identifier}.sh
  # Create a file with the name "run_ops_<identifier>.sh" and add the entry of roachtest run-operation
  drtprod ssh "${WORKLOAD_CLUSTER}":1 -- "tee ${pwd}/${filename} > /dev/null << EOF
#!/bin/bash

export ROACHPROD_GCE_DEFAULT_PROJECT=${ROACHPROD_GCE_DEFAULT_PROJECT}
export ROACHPROD_DNS=${ROACHPROD_DNS}
${pwd}/roachtest-operations run-operation ${CLUSTER} \"${operation_regex}\" --datadog-api-key ${DD_API_KEY} \
--datadog-tags env:development,cluster:${WORKLOAD_CLUSTER},team:drt,service:drt-cockroachdb \
--datadog-app-key 1 --certs-dir ./certs  | tee -a roachtest_ops_${identifier}.log
EOF"
  drtprod ssh "${WORKLOAD_CLUSTER}":1 -- chmod +x "${pwd}"/"${filename}"
  if [ "$cron_config" ]; then
    drtprod run "${WORKLOAD_CLUSTER}":1 -- "(crontab -l; echo \"${cron_config} /usr/bin/flock -n /tmp/lock_${identifier} ${pwd}/${filename}\") | crontab -"
  fi
done

# unmask and start cron
drtprod ssh "${WORKLOAD_CLUSTER}":1 -- sudo systemctl unmask cron
drtprod ssh "${WORKLOAD_CLUSTER}":1 -- sudo systemctl start cron
