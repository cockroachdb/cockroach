#!/usr/bin/env bash
set -euxo pipefail
build/builder.sh make build
build/builder.sh make install

build/builder.sh go test -v -c -tags acceptance ./pkg/acceptance
cd pkg/acceptance

for prop_eval_kv in false true; do
  COCKROACH_PROPOSER_EVALUATED_KV=${prop_eval_kv} ../../acceptance.test -nodes 3 -l ../../artifacts/acceptance -test.v -test.timeout 10m 2>&1 | go-test-teamcity
done
