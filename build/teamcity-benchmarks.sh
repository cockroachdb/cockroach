#!/usr/bin/env bash
set -euo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

build/builder.sh go install ./pkg/cmd/benchmark
build/builder.sh env \
  SERVICE_ACCOUNT_JSON="$SERVICE_ACCOUNT_JSON" \
  benchmark | go-test-teamcity
