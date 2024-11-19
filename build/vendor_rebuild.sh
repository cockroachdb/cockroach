#!/usr/bin/env bash

# Copyright 2020 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -Eeoux pipefail

TMP_VENDOR_DIR=.vendor.tmp.$(date +"%Y-%m-%d.%H%M%S")

trap restore 0

function restore() {
  if [ -d $TMP_VENDOR_DIR ]; then
    rm -rf vendor
    mv $TMP_VENDOR_DIR vendor
  fi
}

if [ -d vendor ]; then
    mv vendor $TMP_VENDOR_DIR
fi
go mod download
go mod vendor
modvendor -copy="**/*.c **/*.h **/*.proto"  -include 'github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis/google/api,github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis/google/rpc,github.com/prometheus/client_model'

rm -rf $TMP_VENDOR_DIR
