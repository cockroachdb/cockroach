# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -euxo pipefail

# Usage: must provide a cross config as argument

if [ -z "$1" ]
then
    echo 'Usage: build.sh CONFIG'
    exit 1
fi

CONFIG="$1"

EXTRA_TARGETS=

# Extra targets to build on Linux x86_64 only.
if [ "$CONFIG" == "crosslinux" ]
then
    EXTRA_TARGETS=$(grep '^//' docs/generated/bazel_targets.txt)
fi

# Extra targets to build on Unix only.
if [ "$CONFIG" != "crosswindows" ]
then
    EXTRA_TARGETS="$EXTRA_TARGETS //pkg/cmd/roachprod //pkg/cmd/workload //pkg/cmd/dev //pkg/cmd/bazci //pkg/cmd/bazci/process-bep-file"
fi

EXTRA_ARGS=
# GEOS does not compile on windows.
GEOS_TARGET=//c-deps:libgeos

if [ "$CONFIG" == "crosswindows" ]
then
   EXTRA_ARGS=--enable_runfiles
   GEOS_TARGET=
fi

bazel build \
    --config "$CONFIG" $EXTRA_ARGS \
    --jobs 100 \
    --build_event_binary_file=bes.bin \
    --bes_keywords ci-build \
    $(./build/github/engflow-args.sh) \
    //pkg/cmd/cockroach-short //pkg/cmd/cockroach \
    //pkg/cmd/cockroach-sql $GEOS_TARGET $EXTRA_TARGETS

