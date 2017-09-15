#!/usr/bin/env bash

# Deletes and recreates the cockroach-nightly resource group, which contains the
# Azure VMs, virtual networks, IPs, etc. that are automatically created by
# nightly tests. This is a big hammer, but the nightlies leak resources
# constantly as `terraform destroy` is not particularly reliable.

set -euo pipefail

docker run --rm --interactive azuresdk/azure-cli-python:2.0.17 bash <<EOF
  set -euo pipefail

  az login --service-principal --username="$ARM_CLIENT_ID" --password="$ARM_CLIENT_SECRET" --tenant="$ARM_TENANT_ID"
  az account set --subscription="$ARM_SUBSCRIPTION_ID"

  # Simulate delete-if-exists behavior.
  az group delete --yes --no-wait --name=cockroach-nightly || true
  az group wait --deleted --timeout=$((60 * 10)) --name=cockroach-nightly

  az group create --location=eastus --name=cockroach-nightly
EOF
