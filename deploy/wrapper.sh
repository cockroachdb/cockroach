#!/bin/sh
# This file serves as an entrypoint for the docker image.
# Its sole purpose is to allow running of "./cockroach test"
# to run the tests, and redirect everything else to the
# cockroach binary.

cd "$(dirname $0)/.."

if [ $# -eq 1 ] && [ "$1" = "test" ]; then
  make test && make testrace
  exit $?
fi
if [ $# -eq 1 ] && [ "$1" = "shell" ]; then
  /bin/bash
  exit $?
fi

./cockroach "$@"
