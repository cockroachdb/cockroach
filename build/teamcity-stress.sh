#!/usr/bin/env bash
set -euxo pipefail

mkdir artifacts

for prop_eval_kv in false true; do
  exit_status=0
  build/builder.sh make stress COCKROACH_PROPOSER_EVALUATED_KV="${prop_eval_kv}" PKG="$PKG" GOFLAGS="${GOFLAGS-}" TAGS="${TAGS-}" TESTTIMEOUT=0 TESTFLAGS='-test.v' STRESSFLAGS='-maxtime 15m -maxfails 1 -stderr' 2>&1 | tee "artifacts/stress.${prop_eval_kv}.log" || exit_status=$?

  if [ $exit_status -ne 0 ]; then
    build/builder.sh env GITHUB_API_TOKEN="$GITHUB_API_TOKEN" BUILD_VCS_NUMBER="$BUILD_VCS_NUMBER" COCKROACH_PROPOSER_EVALUATED_KV="${prop_eval_kv}" github-post < "artifacts/stress.${prop_eval_kv}.log"
    exit $exit_status
  fi;
done

exit 0
