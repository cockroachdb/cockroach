#!/bin/bash

# Copyright 2020 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -e

# Create the necessary directories
mkdir -p /secrets

if [[ ! -f ${ROACHPROD_CENTRALIZED_SECRETS_PATH} ]]; then
    echo "Secrets not found at ${ROACHPROD_CENTRALIZED_SECRETS_PATH}. Please mount the secrets."
    exit 1
fi

# Extract all secrets from the JSON file
jq -r '."gcloud.json"' "${ROACHPROD_CENTRALIZED_SECRETS_PATH}" > /secrets/gcloud.json
AWS_ACCESS_KEY_ID=$(jq -r '.aws_access_key_id' "${ROACHPROD_CENTRALIZED_SECRETS_PATH}")
AWS_SECRET_ACCESS_KEY=$(jq -r '.aws_secret_access_key' "${ROACHPROD_CENTRALIZED_SECRETS_PATH}")
AZURE_USER_ID=$(jq -r '.azure_user_id' "${ROACHPROD_CENTRALIZED_SECRETS_PATH}")
AZURE_PASSWORD=$(jq -r '.azure_password' "${ROACHPROD_CENTRALIZED_SECRETS_PATH}")
AZURE_TENANT_ID=$(jq -r '.azure_tenant_id' "${ROACHPROD_CENTRALIZED_SECRETS_PATH}")
IBM_APIKEY=$(jq -r '.ibm_apikey' "${ROACHPROD_CENTRALIZED_SECRETS_PATH}")


# Check if the gcloud credentials file exists
if [[ -f /secrets/gcloud.json ]]; then
    echo "Found gcloud credentials."
else
    echo "gcloud credentials not found. Please set gcloud credentials in the secrets file."
    exit 1
fi

if [[ -z ${AWS_ACCESS_KEY_ID} || -z ${AWS_SECRET_ACCESS_KEY} ]]; then
    echo "AWS credentials not found. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the secrets file."
    exit 1
fi

if [[ -z ${AZURE_USER_ID} || -z ${AZURE_PASSWORD} || -z ${AZURE_TENANT_ID} ]]; then
    echo "Azure credentials not found. Please set AZURE_USER_ID, AZURE_PASSWORD, and AZURE_TENANT_ID in the secrets file."
    exit 1
fi

if [[ -z ${IBM_APIKEY} ]]; then
    echo "IBM Cloud API key not found. Please set IBM_APIKEY in the secrets file."
    exit 1
fi

# Configure gcloud CLI
#gcloud auth activate-service-account --key-file /secrets/gcloud.json

# Configure AWS CLI
aws configure set aws_access_key_id "${AWS_ACCESS_KEY_ID}"
aws configure set aws_secret_access_key "${AWS_SECRET_ACCESS_KEY}"
aws configure set region us-east-1

# Configure Azure CLI
az login --service-principal -u "${AZURE_USER_ID}" -p "${AZURE_PASSWORD}" -t "${AZURE_TENANT_ID}"

# Configure IBM Cloud
export IBM_APIKEY="${IBM_APIKEY}"

# Set the GOOGLE_APPLICATION_CREDENTIALS environment variable
export GOOGLE_APPLICATION_CREDENTIALS="/secrets/gcloud.json"

exec /usr/local/bin/roachprod-centralized "$@"
