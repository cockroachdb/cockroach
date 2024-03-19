#!/usr/bin/env bash

set -euxo pipefail

pushd cockroach
bazel build //pkg/cmd/cockroach-short \
      --config crosslinux --jobs 100 \
      --bes_keywords integration-test-artifact-build \
      $(./build/github/engflow-args.sh)
cp _bazel/bin/pkg/cmd/cockroach-short/cockroach-short_/cockroach-short ../examples-orms/cockroach
# We need Go in the `PATH`.
export PATH=$(dirname $(bazel run @go_sdk//:bin/go --run_under=realpath)):$PATH
popd

pushd examples-orms
chmod a+w ./cockroach
make dockertest COCKROACH_BINARY=../cockroach
popd
