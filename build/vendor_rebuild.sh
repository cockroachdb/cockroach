#!/usr/bin/env bash

set -Eeoux pipefail

trap restore 1 2 3 6 ERR

function restore() {
  if [ -d .vendor.tmp ]; then
    rm -rf vendor
    mv .vendor.tmp vendor
  fi
}

mv vendor .vendor.tmp
go mod vendor
modvendor -copy="**/*.c **/*.h **/*.proto"  -include 'github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis/google/api,github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis/google/rpc,github.com/prometheus/client_model'
mv .vendor.tmp/.git vendor/.git
rm -rf .vendor.tmp
