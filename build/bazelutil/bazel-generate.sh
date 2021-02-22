#!/usr/bin/env bash

set -exuo pipefail

bazel run //:gazelle -- update-repos -from_file=go.mod -build_file_proto_mode=disable_global -to_macro=DEPS.bzl%go_deps -prune=true
bazel run //pkg/cmd/generate-test-suites --run_under="cd $PWD && " > pkg/BUILD.bazel
bazel run //:gazelle
