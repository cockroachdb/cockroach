#!/bin/sh
#
# Add leaktest.AfterTest(t) to all tests in the given files.
# In addition to running this script, add a main_test.go file similar
# to multiraft/main_test.go (with the package statement changed).
#
# This script is idempotent and should be safe to run on files containing
# a mix of tests with and without AfterTest calls.
#
# Usage: add-leaktest.sh pkg/*_test.go

# Note that go:generate does not do expansion. So "go:generate add-leakest.sh
# *_test.go" will call into here with a single argument of "*_test.go"

set -eu

sed -i'~' -e '
  /^func Test.*(t \*testing.T) {/ {
    # Skip past the test declaration
    n
    # If the next line does not call AfterTest, insert it.
    /leaktest.AfterTest(t)()/! i\
      defer leaktest.AfterTest(t)()
  }
' $@

for i in $@; do
  if ! cmp -s $i $i~ ; then
    # goimports will adjust indentation and add any necessary import.
    goimports -w $i
  fi
  rm -f $i~
done
