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

# TODO(alanmas): So far HOST_TRIPLE is "hardcoded" but
# we need to ensure it gets set correctly as we continue to port things to Bazel.
# TODO(alanmas): As we don’t have a release pipeline set up for Bazel-built cockroach binaries yet
# we are not taking care of:
# - github.com/cockroachdb/cockroach/pkg/build.channel
# - github.com/cockroachdb/cockroach/pkg/util/log.crashReportEnv
# we need to keep this on track to work on them as soon as we release our pipeline.

HOST_TRIPLE="x86_64-pc-linux-gnu"

TARGET_TRIPLE=${HOST_TRIPLE}

# Prefix with STABLE_ so that these values are saved to stable-status.txt
# instead of volatile-status.txt.
# Stamped rules will be retriggered by changes to stable-status.txt, but not by
# changes to volatile-status.txt.
cat <<EOF
STABLE_BUILD_GIT_COMMIT ${GIT_COMMIT-}
STABLE_BUILD_GIT_TAG ${GIT_TAG-}
STABLE_BUILD_GIT_UTCTIME ${GIT_UTCTIME-}
STABLE_BUILD_GIT_BUILD_TYPE ${GIT_BUILD_TYPE-}
STABLE_BUILD_TARGET_TRIPLE ${TARGET_TRIPLE-}
EOF
