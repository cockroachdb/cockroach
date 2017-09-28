#!/usr/bin/env bash

# Deletes and recreates the nightlies resource group, which contains the Azure
# VMs, virtual networks, IPs, disks etc. that are automatically created by
# nightly tests. This is a big hammer, but the nightlies leak resources
# constantly as `terraform destroy` is not particularly reliable.

set -euo pipefail

docker run --rm --interactive \
  --env=ARM_CLIENT_ID \
  --env=ARM_CLIENT_SECRET \
  --env=ARM_TENANT_ID \
  --env=ARM_SUBSCRIPTION_ID \
  azuresdk/azure-cli-python:2.0.17 bash <<'EOF'
  set -euo pipefail

  resource_group=cockroach-nightly

  declare -A storage_accounts=(
    [cockroachnightlyeastvhd]=eastus
    [cockroachnightlywestvhd]=westus
  )

  az login --service-principal --username="$ARM_CLIENT_ID" --password="$ARM_CLIENT_SECRET" --tenant="$ARM_TENANT_ID"
  az account set --subscription="$ARM_SUBSCRIPTION_ID"

  # Simulate delete-if-exists behavior.
  az group delete --yes --no-wait --name="$resource_group" || true
  az group wait --deleted --timeout=$((60 * 10)) --name="$resource_group"

  az group create --location=eastus --name="$resource_group"

  for account_name in "${!storage_accounts[@]}"
  do
    location="${storage_accounts[$account_name]}"
    az storage account create --resource-group="$resource_group" --name="$account_name" --location="$location" --sku=Standard_LRS 
    az storage container create --account-name="$account_name" --name=vhds
  done
EOF
