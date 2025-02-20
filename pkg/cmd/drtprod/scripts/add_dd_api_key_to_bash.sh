#!/bin/bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# this script adds the datadog api key as environment variable "DD_API_KEY" in .bashrc of drt remote VM

if [ -z "${MONITOR_CLUSTER}" ]; then
  echo "environment MONITOR_CLUSTER is not set"
  exit 1
fi


# API key value (replace with your actual key)
DD_API_KEY="$(gcloud --project=cockroach-drt secrets versions access latest --secret datadog-api-key)"

drtprod ssh "${MONITOR_CLUSTER}" -- "echo export DD_API_KEY=\"${DD_API_KEY}\" >> \$HOME/.bashrc"
