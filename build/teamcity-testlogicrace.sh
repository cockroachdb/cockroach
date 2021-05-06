#!/usr/bin/env bash
set -euxo pipefail

export BUILDER_HIDE_GOPATH_SRC=1

source "$(dirname "${0}")/teamcity-support.sh"
maybe_ccache

mkdir -p artifacts

TESTTIMEOUT=24h

run_json_test build/builder.sh \
  stdbuf -oL -eL \
  make testrace \
  GOTESTFLAGS=-json \
  PKG=./pkg/sql/logictest \
  TESTTIMEOUT="${TESTTIMEOUT}"
