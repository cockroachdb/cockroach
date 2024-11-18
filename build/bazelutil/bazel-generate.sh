#!/usr/bin/env bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


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

  # NB: This logic is duplicated in pkg/cmd/dev/test.go. Any changes to the git
  # commands here probably needs to be mirrored there.

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

  # Check if the files are unchanged. This `git diff` will return 1
  # if there are any diffs in the given files.
  git diff --quiet --no-ext-diff $BASE -- "$@" || return 1

  # Finally we have to check if any of the files are untracked; `git diff`
  # won't find those.
  EXTRA=$(git status --porcelain -- "$@")
  if [ -z "$EXTRA" ]; then
    return 0
  fi
  return 1
}

find_relevant() {
    DIR=$1
    shift
    find "$DIR" -name node_modules -prune -o "$@"
}

if files_unchanged_from_upstream go.mod go.sum DEPS.bzl $(find_relevant ./pkg/cmd/mirror/go -name BUILD.bazel -or -name '*.go') $(find_relevant ./pkg/cmd/generate-staticcheck -name BUILD.bazel -or -name '*.go') $(find_relevant ./build/patches -name '*.patch'); then
  echo "Skipping //pkg/cmd/mirror/go:mirror (relevant files are unchanged from upstream)."
  echo "Skipping //pkg/cmd/generate-staticcheck (relevant files are unchanged from upstream)."
else
  CONTENTS=$(bazel run //pkg/cmd/mirror/go:mirror ${EXTRA_BAZEL_ARGS:-})
  echo "$CONTENTS" > DEPS.bzl
  bazel run pkg/cmd/generate-staticcheck --run_under="cd $PWD && " ${EXTRA_BAZEL_ARGS:-}
fi

bazel run //:gazelle ${EXTRA_BAZEL_ARGS:-}

if files_unchanged_from_upstream WORKSPACE $(find_relevant ./pkg/sql/logictest/logictestbase -name BUILD.bazel -or -name '*.go') $(find_relevant ./pkg/sql/logictest/testdata -name '*') $(find_relevant ./pkg/sql/sqlitelogictest -name BUILD.bazel -or -name '*.go') $(find_relevant ./pkg/ccl/logictestccl/testdata -name '*') $(find_relevant pkg/sql/opt/exec/execbuilder/testdata -name '*') $(find_relevant ./pkg/cmd/generate-logictest -name BUILD.bazel -or -name '*.go'); then
  echo "Skipping //pkg/cmd/generate-logictest (relevant files are unchanged from upstream)"
else
  bazel run pkg/cmd/generate-logictest ${EXTRA_BAZEL_ARGS:-} -- -out-dir="$PWD"
fi

if files_unchanged_from_upstream WORKSPACE $(find_relevant ./pkg/acceptance -name '*') $(find_relevant ./pkg/cli/interactive_tests -name '*') $(find_relevant ./pkg/cmd/generate-acceptance-tests -name '*'); then
  echo "Skipping //pkg/cmd/generate-acceptance-tests (relevant files are unchanged from upstream)"
else
  bazel run pkg/cmd/generate-acceptance-tests ${EXTRA_BAZEL_ARGS:-} -- -out-dir="$PWD"
fi

if files_unchanged_from_upstream c-deps/archived.bzl c-deps/REPOSITORIES.bzl DEPS.bzl WORKSPACE $(find_relevant ./pkg/cmd/generate-distdir -name BUILD.bazel -or -name '*.go') $(find_relevant ./pkg/build/bazel -name BUILD.bazel -or -name '*.go') $(find_relevant pkg/build/starlarkutil -name BUILD.bazel -or -name '*.go'); then
    echo "Skipping //pkg/cmd/generate-distdir (relevant files are unchanged from upstream)."
else
    CONTENTS=$(bazel run //pkg/cmd/generate-distdir ${EXTRA_BAZEL_ARGS:-})
    echo "$CONTENTS" > build/bazelutil/distdir_files.bzl
fi

if files_unchanged_from_upstream $(find_relevant ./pkg -name '*.proto') $(find_relevant ./pkg -name BUILD.bazel) $(find_relevant ./pkg -name '*.bzl') $(find_relevant ./docs -name 'BUILD.bazel') $(find_relevant ./docs -name '*.bzl') $(find_relevant ./pkg/gen/genbzl -name '*.go'); then
  echo "Skipping //pkg/gen/genbzl (relevant files are unchanged from upstream)."
else
  bazel run pkg/gen/genbzl --run_under="cd $PWD && " ${EXTRA_BAZEL_ARGS:-} -- --out-dir pkg/gen
fi

if files_unchanged_from_upstream $(find_relevant ./pkg -name BUILD.bazel) $(find_relevant ./pkg/cmd/generate-bazel-extra -name BUILD.bazel -or -name '*.go'); then
  echo "Skipping //pkg/cmd/generate-bazel-extra (relevant files are unchanged from upstream)."
else
  bazel run //pkg/cmd/generate-bazel-extra --run_under="cd $PWD && " ${EXTRA_BAZEL_ARGS:-}
fi
