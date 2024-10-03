#!/bin/bash

# Copyright 2020 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -e

# Unpack all of the keys, configs, etc. and then run roachprod
gcloud auth activate-service-account --key-file /secrets/gcloud.json
aws configure set aws_access_key_id $(cat /secrets/aws_access_key_id)
aws configure set aws_secret_access_key $(cat /secrets/aws_secret_access_key)
# The default profile has to contain a region name in order for the AWS Go SDK
# library to work
aws configure set region us-east-1
az login --service-principal -u $(cat /secrets/azure_user_id) -p $(cat /secrets/azure_password) -t $(cat /secrets/azure_tenant_id)
/usr/local/bin/roachprod "$@"
# Report only successful runs to Dead Man's Snitch
curl https://nosnch.in/060eb36e55
