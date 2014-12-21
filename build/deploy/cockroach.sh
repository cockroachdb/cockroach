#!/bin/sh
set -e
if [ -f ./test.sh ]; then
  ./test.sh || exit $?
fi
/cockroach/cockroach "$@"
