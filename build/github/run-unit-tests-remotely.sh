#!/usr/bin/env bash

set -euxo pipefail

echo "$ENGFLOW_CERT_CRT" > "$RUNNER_TEMP/engflow.crt"
echo "$ENGFLOW_CERT_CRT" | sha256sum
echo "length of cert crt is ${#ENGFLOW_CERT_CRT}"
echo "$ENGFLOW_CERT_KEY" > "$RUNNER_TEMP/engflow.key"
sed -i 's|/home/agent/engflow/|'$RUNNER_TEMP'/|g' .bazelrc
bazel test --config cinolint --config engflow --//build/toolchains:cross_flag pkg:all_tests --jobs 200
