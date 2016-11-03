#!/usr/bin/env bash
set -euxo pipefail
for prop_eval_kv in false true; do
  build/builder.sh make test COCKROACH_PROPOSER_EVALUATED_KV="${prop_eval_kv}" TESTFLAGS='-v' 2>&1 | go-test-teamcity
done

build/builder.sh env BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" TARGET=stress github-pull-request-make
