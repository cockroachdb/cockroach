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

# We don't have to exercise the checks below on every configuration, only
# bother with Linux.
if [ "$CONFIG" != "crosslinux" ]
then
    exit 0
fi

# This is going to be very noisy if we don't disbale this logging.
set +x
BAZEL_BIN=$(bazel info bazel-bin --config "$CONFIG")
FAILED=
# Ensure all generated docs are byte-for-byte identical with the checkout.
for FILE in $(find $BAZEL_BIN/docs -type f)
do
    RESULT=$(diff $FILE ${FILE##$BAZEL_BIN/})
    if [[ ! $? -eq 0 ]]
    then
        echo "File $FILE does not match with checked-in version. Got diff:"
        echo "$RESULT"
        echo "Run './dev generate docs'"
        FAILED=1
    fi
done
# Ensure the generated docs are inclusive of what we have in tree: list all
# generated files in a few subdirectories and make sure they're all in the
# build output.
for FILE in $(ls docs/generated/http/*.md | xargs -n1 basename)
do
    if [[ ! -f $BAZEL_BIN/docs/generated/http/$FILE ]]
    then
        echo "File $BAZEL_BIN/docs/generated/http/$FILE does not exist as a generated artifact; it is checked in under `docs/generated/http`, but is not being built. Is docs/generated/http/BUILD.bazel up-to-date?"
        FAILED=1
    fi
done
for FILE in $(ls docs/generated/sql/*.md | xargs -n1 basename)
do
    if [[ ! -f $BAZEL_BIN/docs/generated/sql/$FILE ]]
    then
        echo "File $BAZEL_BIN/docs/generated/sql/$FILE does not exist as a generated artifact; it is checked in under `docs/generated/sql`, but is not being built. Is docs/generated/sql/BUILD.bazel up-to-date?"
        FAILED=1
    fi
done
for FILE in $(ls docs/generated/sql/bnf/*.bnf | xargs -n1 basename)
do
    if [[ ! -f $BAZEL_BIN/docs/generated/sql/bnf/$FILE ]]
    then
        echo "File $BAZEL_BIN/docs/generated/sql/bnf/$FILE does not exist as a generated artifact; it is checked in under `docs/generated/sql/bnf`, but is not being built. Is docs/generated/sql/bnf/BUILD.bazel up-to-date?"
        FAILED=1
    fi
done

if [[ ! -z "$FAILED" ]]
then
    echo 'Generated files do not match! Are the checked-in generated files up-to-date?'
    exit 1
fi
