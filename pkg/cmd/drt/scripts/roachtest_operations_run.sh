#! /bin/bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


cd ~

while true; do
  ./roachtest-operations run-operation ".*" --certs-dir ./certs --cluster "cct-232" --cockroach-binary "cockroach" --virtual-cluster "application" | tee -a roachtest_ops.log
  sleep 10
done
