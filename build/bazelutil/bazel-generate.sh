#!/usr/bin/env bash

set -exuo pipefail

bazel run --symlink_prefix=.bazel/ //:gazelle -- update-repos -from_file=go.mod -build_file_proto_mode=disable_global -to_macro=DEPS.bzl%go_deps -prune=true
bazel run --symlink_prefix=.bazel/ //pkg/cmd/generate-test-suites --symlink_prefix=.bazel/ --run_under="cd $PWD && " > pkg/BUILD.bazel
bazel run --symlink_prefix=.bazel/ //:gazelle
