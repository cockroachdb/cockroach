#!/bin/bash
set -e

# Unpack all of the keys, configs, etc. and then start roachperf
gcloud auth activate-service-account --key-file /secrets/gcloud.json
aws configure set aws_access_key_id $(cat /secrets/aws_access_key_id)
aws configure set aws_secret_access_key $(cat /secrets/aws_secret_access_key)
az login --service-principal -u $(cat /secrets/azure_user_id) -p $(cat /secrets/azure_password) -t $(cat /secrets/azure_tenant_id)
exec roachprod $@
