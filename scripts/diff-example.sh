#!/usr/bin/env bash

# Diff a failed Go example [0] against its expected output.
#
# By default, when an example fails, Go prints "got:", followed by the
# actual output, then prints "want:", followed by the expected output. When the
# output grows beyond several lines, as it often does, it's virtually impossible
# to visually parse where the output differs, especially when whitespace is
# involved. This script parses the output and prints it as a unified diff to
# make the difference obvious.
#
# If this script produces no output, it means the actual and expected output
# matched.
#
# Sample usage:
#
#     $ make test PKG=./pkg/cli TESTS=Example_zone | scripts/diff-example.sh
#
# [0]: https://golang.org/pkg/testing/#hdr-Examples

set -euo pipefail

do_diff() {
  if [ -x "$(command -v colordiff)" ]; then
    colordiff "$@"
  else
    diff "$@"
  fi
}

cd $(mktemp -d)
trap 'rm -rf $PWD' EXIT

cat >in

# The actual output is everything after "got:" but before "want:".
<in sed -n  "
 /^got:$/,/^want:$/{
   //d
   p
}" >got

# The desired output is everything after "want:", excluding the status about
# whether the test passed or failed.
<in sed "1,/^want:$/d" | grep -Ev "^(PASS|FAIL)" >want

do_diff -u want got
