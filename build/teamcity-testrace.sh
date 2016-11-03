#!/usr/bin/env bash
set -euxo pipefail

for prop_eval_kv in false true; do
  build/builder.sh make testrace COCKROACH_PROPOSER_EVALUATED_KV="${prop_eval_kv}" TESTFLAGS='-v' 2>&1 | go-test-teamcity
  build/builder.sh env COCKROACH_PROPOSER_EVALUATED_KV="${prop_eval_kv}" BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" TARGET=stressrace github-pull-request-make
done
