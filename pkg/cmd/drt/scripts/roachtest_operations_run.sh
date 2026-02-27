#! /bin/bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# Usage: ./roachtest_operations_run.sh <cluster> <workload_cluster> <cloud>
# Example: ./roachtest_operations_run.sh drt-chaos-aws workload-chaos-aws aws
# Example: ./roachtest_operations_run.sh drt-chaos workload-chaos gcp

set -euo pipefail

CLUSTER="${1:-}"
WORKLOAD_CLUSTER="${2:-}"
CLOUD="${3:-}"

if [ -z "${CLUSTER}" ] || [ -z "${WORKLOAD_CLUSTER}" ] || [ -z "${CLOUD}" ]; then
  echo "Usage: $0 <cluster> <workload_cluster> <cloud>"
  echo "  cloud: aws or gcp"
  echo ""
  echo "Examples:"
  echo "  $0 drt-chaos-aws workload-chaos-aws aws"
  echo "  $0 drt-chaos workload-chaos gcp"
  exit 1
fi

cd /home/ubuntu

export ROACHPROD_GCE_DEFAULT_PROJECT=cockroach-drt
export ROACHPROD_DNS="drt.crdb.io"
./roachprod sync
sleep 20

# Fetch secrets from cloud provider at runtime (not stored in any file)
# AWS secrets are stored in us-east-1 region for consistency across all clusters
AWS_SECRETS_REGION="us-east-1"

fetch_secret() {
  local secret_name="$1"
  local secret_value=""

  case "${CLOUD}" in
    aws)
      if ! secret_value="$(aws secretsmanager get-secret-value \
        --region "${AWS_SECRETS_REGION}" \
        --secret-id "${secret_name}" \
        --query SecretString \
        --output text 2>&1)"; then
        echo "Error: Failed to fetch secret '${secret_name}' from AWS Secrets Manager (region: ${AWS_SECRETS_REGION})" >&2
        echo "Details: ${secret_value}" >&2
        exit 1
      fi
      ;;
    gcp)
      if ! secret_value="$(gcloud --project=cockroach-drt secrets versions access latest \
        --secret "${secret_name}" 2>&1)"; then
        echo "Error: Failed to fetch secret '${secret_name}' from GCP Secret Manager" >&2
        echo "Details: ${secret_value}" >&2
        exit 1
      fi
      ;;
    *)
      echo "Error: Unknown cloud '${CLOUD}'. Must be 'aws' or 'gcp'." >&2
      exit 1
      ;;
  esac

  if [ -z "${secret_value}" ]; then
    echo "Error: Secret '${secret_name}' exists but has an empty value" >&2
    exit 1
  fi

  echo "${secret_value}"
}

DD_API_KEY="$(fetch_secret datadog-api-key)"

if [ -z "${DD_API_KEY}" ]; then
  echo "Error: Datadog API key is empty" >&2
  exit 1
fi

while true; do
  ./roachtest-operations run-operation "${CLUSTER}" ".*" \
    --datadog-api-key "${DD_API_KEY}" \
    --datadog-tags "env:development,cluster:${WORKLOAD_CLUSTER},team:drt,service:drt-cockroachdb" \
    --certs-dir ./certs \
    --cloud aws --workload-cluster "${WORKLOAD_CLUSTER}" | tee -a roachtest_ops.log
  sleep 600
done
