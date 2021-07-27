#!/usr/bin/env bash

set -euo pipefail

source "$(dirname "${0}")/teamcity-support.sh"  # For $root
source "$(dirname "${0}")/teamcity-bazel-support.sh"  # For run_bazel

tc_prepare

tc_start_block "Run Bazel build"
run_bazel build/bazelutil/bazelbuild.sh crosslinux
tc_end_block "Run Bazel build"

set +e

tc_start_block "Ensure generated files match"
FAILED=
for FILE in $(find $root/artifacts/bazel-bin/docs -type f)
do
    RESULT=$(diff $FILE $root/${FILE##$root/artifacts/bazel-bin/})
    if [[ ! $? -eq 0 ]]
    then
        echo "File $FILE does not match with checked-in version. Got diff:"
        echo "$RESULT"
        FAILED=1
    fi
done
if [[ ! -z "$FAILED" ]]
then
    echo 'Generated files do not match! Are the checked-in generated files up-to-date?'
    exit 1
fi
tc_end_block "Ensure generated files match"
