#!/usr/bin/env bash

set -euo pipefail

# Even with --symlink_prefix, some sub-command somewhere hardcodes the
# creation of a "bazel-out" symlink. This bazel-out symlink can only
# be blocked by the existence of a file before the bazel command is
# invoked. For now, this is left as an exercise for the user.

CONTENTS=$(bazel run //pkg/cmd/mirror)
echo "$CONTENTS" > DEPS.bzl
bazel run pkg/cmd/generate-staticcheck --run_under="cd $PWD && "
bazel run //:gazelle
CONTENTS=$(bazel run //pkg/cmd/generate-test-suites --run_under="cd $PWD && ")
echo "$CONTENTS" > pkg/BUILD.bazel
