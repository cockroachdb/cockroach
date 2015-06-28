#!/bin/sh
# This file serves as an entrypoint for the docker image,
# allowing injection of shell commands.

set -eu

cd "$(dirname $0)"

if [ "$1" = "test" ] || [ "$1" = "testrace" ]; then
  make "$@"
  exit $?
elif [ "$1" = "shell" ]; then
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
