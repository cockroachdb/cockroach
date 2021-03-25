#!/usr/bin/env bash

# This command is used by bazel as the workspace_status_command
# to implement build stamping with git information.

set -o errexit
set -o nounset
set -o pipefail

# Do not use plumbing commands, like git diff-index, in this target. Our build
# process modifies files quickly enough that plumbing commands report false
# positives on filesystems with only one second of resolution as a performance
# optimization. Porcelain commands, like git diff, exist to detect and remove
# these false positives.
#
# For details, see the "Possible timestamp problems with diff-files?" thread on
# the Git mailing list (http://marc.info/?l=git&m=131687596307197).

BUILDTYPE="development"
LDFLAGS=""

GIT_BUILD_TYPE=(${BUILDTYPE} -extldflags "${LDFLAGS}")

GIT_COMMIT=$(git rev-parse HEAD)
GIT_TAG=$(git describe --tags --dirty --match=v[0-9]* 2> /dev/null || git rev-parse --short HEAD;)
GIT_UTCTIME=$(date -u '+%Y/%m/%d %H:%M:%S')

# Prefix with STABLE_ so that these values are saved to stable-status.txt
# instead of volatile-status.txt.
# Stamped rules will be retriggered by changes to stable-status.txt, but not by
# changes to volatile-status.txt.
cat <<EOF
STABLE_BUILD_GIT_COMMIT ${GIT_COMMIT-}
STABLE_BUILD_GIT_TAG ${GIT_TAG-}
STABLE_BUILD_GIT_UTCTIME ${GIT_UTCTIME-}
STABLE_BUILD_GIT_BUILD_TYPE ${GIT_BUILD_TYPE-}
EOF
