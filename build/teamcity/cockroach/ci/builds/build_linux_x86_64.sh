#!/usr/bin/env bash

set -euo pipefail

dir="$(dirname $(dirname $(dirname $(dirname $(dirname "${0}")))))"

source "$dir/teamcity-support.sh"  # For $root
source "$dir/teamcity-bazel-support.sh"  # For run_bazel

tc_prepare

tc_start_block "Run Bazel build"
run_bazel build/teamcity/cockroach/ci/builds/build_impl.sh crosslinux
tc_end_block "Run Bazel build"

set +e

tc_start_block "Ensure generated files match"
FAILED=
# Ensure all generated docs are byte-for-byte identical with the checkout.
for FILE in $(find $root/artifacts/bazel-bin/docs -type f)
do
    RESULT=$(diff $FILE $root/${FILE##$root/artifacts/bazel-bin/})
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
for FILE in $(ls $root/docs/generated/http/*.md | xargs -n1 basename)
do
    if [[ ! -f $root/artifacts/bazel-bin/docs/generated/http/$FILE ]]
    then
        echo "File $root/artifacts/bazel-bin/docs/generated/http/$FILE does not exist as a generated artifact. Is docs/generated/http/BUILD.bazel up-to-date?"
        FAILED=1
    fi
done
for FILE in $(ls $root/docs/generated/sql/*.md | xargs -n1 basename)
do
    if [[ ! -f $root/artifacts/bazel-bin/docs/generated/sql/$FILE ]]
    then
        echo "File $root/artifacts/bazel-bin/docs/generated/sql/$FILE does not exist as a generated artifact. Is docs/generated/sql/BUILD.bazel up-to-date?"
        FAILED=1
    fi
done
for FILE in $(ls $root/docs/generated/sql/bnf/*.bnf | xargs -n1 basename)
do
    if [[ ! -f $root/artifacts/bazel-bin/docs/generated/sql/bnf/$FILE ]]
    then
        echo "File $root/artifacts/bazel-bin/docs/generated/sql/bnf/$FILE does not exist as a generated artifact. Is docs/generated/sql/bnf/BUILD.bazel up-to-date?"
        FAILED=1
    fi
done
# Ensure checked-in generated files are byte-for-byte identical with what's in
# the checkout.
for LINE in $(grep -v '^#' $root/build/bazelutil/checked_in_genfiles.txt)
do
    target=$(echo $LINE | cut -d'|' -f1)
    dir=$(echo $target | sed 's|//||g' | cut -d: -f1)
    old_basename=$(echo $LINE | cut -d'|' -f2)
    new_basename=$(echo $LINE | cut -d'|' -f3)
    RESULT=$(diff $root/artifacts/bazel-bin/$dir/$old_basename $root/$dir/$new_basename)
    if [[ ! $? -eq 0 ]]
    then
        echo "Generated file $dir/$new_basename does not match with checked-in version. Got diff:"
        echo "$RESULT"
        echo "Run './dev generate go'"
        FAILED=1
    fi
done
# docs/generated/redact_safe.md needs special handling.
REAL_REDACT_SAFE=$($root/build/bazelutil/generate_redact_safe.sh)
RESULT=$(diff <(echo "$REAL_REDACT_SAFE") $root/docs/generated/redact_safe.md)
if [[ ! $? -eq 0 ]]
then
    echo "docs/generated/redact_safe.md is not up-to-date. Run './dev generate docs'"
fi

if [[ ! -z "$FAILED" ]]
then
    echo 'Generated files do not match! Are the checked-in generated files up-to-date?'
    exit 1
fi
tc_end_block "Ensure generated files match"
