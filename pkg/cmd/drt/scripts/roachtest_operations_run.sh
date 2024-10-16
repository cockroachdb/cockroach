#! /bin/bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

cd /home/ubuntu

export ROACHPROD_GCE_DEFAULT_PROJECT=cockroach-drt
export ROACHPROD_DNS="drt.crdb.io"
./roachprod sync
sleep 20

# add --datadog-api-key and --datadog-app-key
while true; do
  ./roachtest-operations run-operation "drt-scale" ".*" --datadog-tags env:development,cluster:workload-scale,team:drt,service:drt-cockroachdb --certs-dir ./certs  | tee -a roachtest_ops.log
  sleep 600
done
