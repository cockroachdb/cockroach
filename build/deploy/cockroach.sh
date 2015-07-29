#!/bin/sh

set -eu

if [ "${1-}" = "shell" ]; then
  shift
  /bin/sh "$@"
else
  /cockroach/cockroach "$@"
fi
