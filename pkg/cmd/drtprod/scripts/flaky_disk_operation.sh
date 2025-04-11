#!/bin/bash

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

filename="run_ops_disk_stall.sh"
drtprod ssh "${WORKLOAD_CLUSTER}":1 -- "tee /home/ubuntu/${filename} > /dev/null << 'EOF'
#!/bin/bash

export ROACHPROD_GCE_DEFAULT_PROJECT=${ROACHPROD_GCE_DEFAULT_PROJECT}
export ROACHPROD_DNS=${ROACHPROD_DNS}

for i in {1..50}; do
  echo \"Running disk-stall iteration \$i/50...\"
  /home/ubuntu/roachtest run-operation ${CLUSTER} \"disk-stall\" --datadog-api-key ${DD_API_KEY} \
  --datadog-tags env:development,cluster:${WORKLOAD_CLUSTER},team:drt,service:drt-cockroachdb \
  --datadog-app-key 1 --certs-dir ./certs --wait-before-cleanup 10s | tee -a roachtest_ops_disk_stall.log

  SLEEP_TIME=\$((10 + RANDOM % 11))
  echo \"Sleeping for \$SLEEP_TIME seconds before next iteration...\"
  sleep \$SLEEP_TIME
done
EOF"


drtprod ssh "${WORKLOAD_CLUSTER}":1 -- chmod +x /home/ubuntu/"${filename}"
