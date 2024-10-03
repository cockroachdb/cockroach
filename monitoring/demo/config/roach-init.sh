#!/usr/bin/env bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


# curl health endpoint
while ! health=$(curl -s "http://roach1:8080/health?ready=1"); do
  sleep 0.1
done

# check if health check shows uninitialized cluster
error='"error": "node is waiting for cluster initialization"'
if [[ $health =~ $error ]]; then
  ./cockroach init --insecure --host=roach1:26257
else
  echo "Cluster is up!"
fi
