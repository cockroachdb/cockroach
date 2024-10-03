#!/usr/bin/env bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


# This command is used by bazel as the workspace_status_command
# to implement build stamping with git information.

# Usage: stamp.sh [target-triple] [build-channel] [build-type]
# All arguments are optional and have appropriate defaults. In this way,
# stamp.sh with no arguments is appropriate as the `workplace_status_command`
# for a development build.
#  target-triple: defaults to the value of `cc -dumpmachine`
#  build-channel: defaults to `unknown`, but can be `official-binary`
#  build-type: defaults to `development`, but can be `release`
#  build-tag: will default to an appropriate value if not passed in, but can be overridden

set -euo pipefail

# Do not use plumbing commands, like git diff-index, in this target. Our build
# process modifies files quickly enough that plumbing commands report false
# positives on filesystems with only one second of resolution as a performance
# optimization. Porcelain commands, like git diff, exist to detect and remove
# these false positives.
#
# For details, see the "Possible timestamp problems with diff-files?" thread on
# the Git mailing list (http://marc.info/?l=git&m=131687596307197).

# Handle target-triple.
if [ -z "${1+x}" ]
then
    TARGET_TRIPLE=$(cc -dumpmachine)
else
    TARGET_TRIPLE="$1"
    shift 1
fi

# Handle build-channel.
if [ -z "${1+x}" ]
then
    BUILD_CHANNEL="unknown"
else
    BUILD_CHANNEL="$1"
    shift 1
fi

# Handle build-type.
if [ -z "${1+x}" ]
then
    BUILD_TYPE="development"
else
    BUILD_TYPE="$1"
    shift 1
fi

if [ "$BUILD_TYPE" = "release" ]
then
    CRASH_REPORT_ENV=$(cat ./pkg/build/version.txt)
else
    CRASH_REPORT_ENV="development"
fi

# Handle build-tag.
if [ -z "${1+x}" ]
then
    BUILD_TAG=
else
    BUILD_TAG="$1"
    shift 1
fi

BUILD_REV=$(git describe --match="" --always --abbrev=40)
if [[ -n "$(git status -s --ignore-submodules --untracked-files=no)" ]]; then
    BUILD_REV=$BUILD_REV-dirty
fi

BUILD_UTCTIME=$(date -u '+%Y/%m/%d %H:%M:%S')


# Variables beginning with "STABLE" will be written to stable-status.txt, and
# others will be written to volatile-status.txt.
# Go binaries will be re-linked by Bazel upon changes to stable-status.txt, but
# not if only volatile-status.txt has changed.
# Ref:
# * https://docs.bazel.build/versions/main/user-manual.html#workspace_status
# * https://github.com/bazelbuild/rules_go/blob/master/go/core.rst#defines-and-stamping
cat <<EOF
STABLE_BUILD_CHANNEL ${BUILD_CHANNEL-}
STABLE_BUILD_TAG ${BUILD_TAG-}
STABLE_BUILD_TARGET_TRIPLE ${TARGET_TRIPLE-}
STABLE_BUILD_TYPE ${BUILD_TYPE-}
STABLE_CRASH_REPORT_ENV ${CRASH_REPORT_ENV-}
BUILD_REV ${BUILD_REV-}
BUILD_UTCTIME ${BUILD_UTCTIME-}
EOF
