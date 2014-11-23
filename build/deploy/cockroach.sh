#!/bin/sh
set -e
if [ -f /test/test.sh ]; then
  (cd /test && ./test.sh) || exit $?
fi
/cockroach/cockroach "$@"
