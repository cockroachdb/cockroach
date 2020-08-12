#!/usr/bin/env bash

set -Eeoux pipefail

TMP_VENDOR_DIR=.vendor.tmp.$(date +"%Y-%m-%d.%H%M%S")

trap restore 0

function restore() {
  if [ -d $TMP_VENDOR_DIR ]; then
    rm -rf vendor
    mv $TMP_VENDOR_DIR vendor
  fi
}

mv vendor $TMP_VENDOR_DIR
go mod vendor
modvendor -copy="**/*.c **/*.h **/*.proto"  -include 'github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis/google/api,github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis/google/rpc,github.com/prometheus/client_model'
mv $TMP_VENDOR_DIR/.git vendor/.git
rm -rf $TMP_VENDOR_DIR
