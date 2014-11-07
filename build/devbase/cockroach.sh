#!/bin/sh
# This file serves as an entrypoint for the docker image.
# Its purpose is to allow running of "./cockroach test"
# to run the tests.
cd "$(dirname $0)"

if [ $# -eq 1 ] && [ "$1" = "test" ]; then
  make test && make testrace
  exit $?
fi
if [ $# -ge 1 ] && [ "$1" = "shell" ]; then
  shift
  if [ $# -eq 0 ]; then
    /bin/bash
  else
    CMD="$@"
    /bin/bash -c "${CMD}"
  fi
  exit $?
fi

./cockroach "$@"
