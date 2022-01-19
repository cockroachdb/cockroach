#!/usr/bin/env bash

# This command is used by bazel as the workspace_status_command
# to implement build stamping with git information.

set -euo pipefail

# Do not use plumbing commands, like git diff-index, in this target. Our build
# process modifies files quickly enough that plumbing commands report false
# positives on filesystems with only one second of resolution as a performance
# optimization. Porcelain commands, like git diff, exist to detect and remove
# these false positives.
#
# For details, see the "Possible timestamp problems with diff-files?" thread on
# the Git mailing list (http://marc.info/?l=git&m=131687596307197).

GIT_BUILD_TYPE="development"
GIT_COMMIT=$(git rev-parse HEAD)
GIT_TAG=$(git describe --tags --dirty --match=v[0-9]* 2> /dev/null || git rev-parse --short HEAD;)
GIT_UTCTIME=$(date -u '+%Y/%m/%d %H:%M:%S')

if [ -z "${1+x}" ]
then
    TARGET_TRIPLE=$(cc -dumpmachine)
else
    TARGET_TRIPLE="$1"
fi

# TODO(ricky): Also provide a way to stamp the following variables:
# - github.com/cockroachdb/cockroach/pkg/build.channel
# - github.com/cockroachdb/cockroach/pkg/util/log.crashReportEnv

# Variables beginning with "STABLE" will be written to stable-status.txt, and
# others will be written to volatile-status.txt.
# Go binaries will be re-linked by Bazel upon changes to stable-status.txt, but
# not if only volatile-status.txt has changed.
# Ref:
# * https://docs.bazel.build/versions/main/user-manual.html#workspace_status
# * https://github.com/bazelbuild/rules_go/blob/master/go/core.rst#defines-and-stamping
cat <<EOF
STABLE_BUILD_GIT_BUILD_TYPE ${GIT_BUILD_TYPE-}
STABLE_BUILD_TARGET_TRIPLE ${TARGET_TRIPLE-}
BUILD_GIT_COMMIT ${GIT_COMMIT-}
BUILD_GIT_TAG ${GIT_TAG-}
BUILD_GIT_UTCTIME ${GIT_UTCTIME-}
EOF
