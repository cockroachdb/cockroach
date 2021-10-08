#!/usr/bin/env bash

set -euo pipefail

# Even with --symlink_prefix, some sub-command somewhere hardcodes the
# creation of a "bazel-out" symlink. This bazel-out symlink can only
# be blocked by the existence of a file before the bazel command is
# invoked. For now, this is left as an exercise for the user.

bazel run //:gazelle -- update-repos -from_file=go.mod -build_file_proto_mode=disable_global -to_macro=DEPS.bzl%go_deps -prune=true
CONTENTS=$(bazel run //pkg/cmd/generate-test-suites --run_under="cd $PWD && ")
echo "$CONTENTS" > pkg/BUILD.bazel
bazel run //:gazelle
