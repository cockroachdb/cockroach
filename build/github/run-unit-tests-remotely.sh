#!/usr/bin/env bash

set -euo pipefail

echo "$ENGFLOW_CERT_CRT" > "$RUNNER_TEMP/engflow.crt"
echo "$ENGFLOW_CERT_KEY" > "$RUNNER_TEMP/engflow.key"

set -x

sed -i 's|/home/agent/engflow/|'$RUNNER_TEMP'/|g' .bazelrc
bazel build --lintonbuild --config engflow --//build/toolchains:cross_flag pkg/cmd/cockroach-short --jobs 200
