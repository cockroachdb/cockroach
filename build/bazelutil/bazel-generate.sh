#!/usr/bin/env bash

set -euo pipefail

# files_unchanged_from_upstream takes file globs as arguments and checks
# whether all these files are unchanged from the upstream master branch.
# It is best effort.
# This optimization can be disabled by setting env var
# COCKROACH_BAZEL_FORCE_GENERATE=1.
files_unchanged_from_upstream () {
  if [ "${COCKROACH_BAZEL_FORCE_GENERATE:-}" = 1 ]; then
    return 1
  fi
  
  if ! which git >/dev/null; then
    return 1
  fi

  # First, figure out the correct remote.
  UPSTREAM=$(git remote -v | grep 'github.com[/:]cockroachdb/cockroach.*(fetch)' | awk '{print $1}') || return 1
  if [ -z "$UPSTREAM" ]; then
    return 1
  fi

  # Find the upstream commit which this branch is based on.
  BASE=$(git merge-base $UPSTREAM/master HEAD 2>/dev/null) || return 1
  if [ -z "$BASE" ]; then
    return 1
  fi

  # Check if the files are unchanged.
  DIFF=$(git diff --no-ext-diff --name-only $BASE -- "$@") || return 1
  if [ -z "$DIFF" ]; then
    # No diffs.
    return 0
  fi
  return 1
}

# Even with --symlink_prefix, some sub-command somewhere hardcodes the
# creation of a "bazel-out" symlink. This bazel-out symlink can only
# be blocked by the existence of a file before the bazel command is
# invoked. For now, this is left as an exercise for the user.

if files_unchanged_from_upstream go.mod go.sum DEPS.bzl $(find ./pkg/cmd/mirror -name BUILD.bazel -or -name '*.go') $(find ./pkg/cmd/generate-staticcheck -name BUILD.bazel -or -name '*.go') $(find ./build/patches -name '*.patch'); then
  echo "Skipping //pkg/cmd/mirror (relevant files are unchanged from upstream)."
  echo "Skipping //pkg/cmd/generate-staticcheck (relevant files are unchanged from upstream)."
else
  CONTENTS=$(bazel run //pkg/cmd/mirror)
  echo "$CONTENTS" > DEPS.bzl
  bazel run pkg/cmd/generate-staticcheck --run_under="cd $PWD && "
fi

bazel run //:gazelle

if files_unchanged_from_upstream $(find ./pkg -name '*.proto') $(find ./pkg -name BUILD.bazel) $(find ./pkg -name '*.bzl') $(find ./docs -name 'BUILD.bazel') $(find ./docs -name '*.bzl') $(find ./pkg/gen/genbzl -name '*.go'); then
  echo "Skipping //pkg/gen/genbzl (relevant files are unchanged from upstream)."
else
  bazel run pkg/gen/genbzl --run_under="cd $PWD && " -- --out-dir pkg/gen
fi

if files_unchanged_from_upstream $(find ./pkg -name BUILD.bazel) $(find ./pkg -name '*.bzl'); then
  echo "Skipping //pkg/cmd/generate-test-suites (relevant files are unchanged from upstream)."
else
  CONTENTS=$(bazel run //pkg/cmd/generate-test-suites --run_under="cd $PWD && ")
  echo "$CONTENTS" > pkg/BUILD.bazel
fi
