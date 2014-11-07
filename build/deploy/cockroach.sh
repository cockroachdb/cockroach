#!/bin/sh
set -ex
if [ -f /test/test.sh ]; then
  2>&1 echo "running tests"
  (cd /test && ./test.sh) || exit $?
fi
/cockroach/cockroach "$@"
