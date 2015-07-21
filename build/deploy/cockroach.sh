#!/bin/sh

set -eu

if [ -f ../build/test.sh ]; then
  ../build/test.sh
fi

if [ "${1:-}" = "shell" ]; then
  shift
  /bin/bash "$@"
else
  /cockroach/cockroach "$@"
fi
