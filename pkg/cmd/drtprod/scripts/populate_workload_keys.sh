#!/bin/bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

export ROACHPROD_DISABLED_PROVIDERS=IBM

if [ -z "${CLUSTER}" ]; then
  echo "environment CLUSTER is not set"
  exit 1
fi

if [ -z "${WORKLOAD_CLUSTER}" ]; then
  echo "environment CLUSTER is not set"
  exit 1
fi

# the ssh keys of all workload nodes should be setup on the crdb nodes for the operations
drtprod ssh ${CLUSTER} -- "echo \"$(drtprod run ${WORKLOAD_CLUSTER}:1 -- cat ./.ssh/id_rsa.pub|grep ssh-rsa)\" >> ./.ssh/authorized_keys"
