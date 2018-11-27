#!/usr/bin/env bash

# werror.sh promotes warnings produced by a command to fatal errors, just like
# GCC's -Werror flag.
#
# More formally, `werror.sh COMMAND [ARGS...]` invokes COMMAND with the
# specified ARGS. If any output is produced on stdout or stderr, werror.sh
# prints both streams to stderr and exits with a failing exit code. Otherwise,
# werror.sh exits with the exit code of COMMAND.
#
# werror.sh is a blunt instrument and does not attempt to distinguish between
# output that represents a warning and output that does not. Prefer to use a
# -Werror flag if available.

output=$("$@" 2>&1)
exit_code=$?
if [[ "$output" ]]; then
  echo "$output" >&2
  echo "fatal: treating warnings from $(basename $1) as errors"
  exit 1
fi
exit "$exit_code"
