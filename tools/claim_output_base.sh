#!/bin/bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


# Typically only one bazel server can be running in one output_base at
# a time, which means if we always use the default base, only one bazel server 
# can be running for a given workspace at a time. Instead, we'll keep a few 
# output bases and claim an available one as needed to allow concurrent runs.
#
# If we run out of bases and fall back to the default, or somehow end up handing
# the same base to two runs, bazel will resort to queuing on its own locking 
# meaning this claim system is not important to correctness and is just here to
# make life easier during development.
#
# If this script successfully claims a base, it will export the its path in the 
# var BAZEL_OUTPUT_BASE, and install an EXIT trap to release it. Scripts which
# call this script should thus avoid using `exec` as that prevents the cleanup
# trap from running as it replaces the entire process.

# If a base has already been claimed, there's nothing more to do.
if [ ! -z "${BAZEL_OUTPUT_BASE-}" ]; then
  return
fi

if which md5 > /dev/null; then 
  SUFFIX="$(pwd | md5 | head -c6)"        
else 
  SUFFIX="$(pwd | md5sum | head -c6)"
fi 

if [ -d "${HOME}/.cache/bazel/_bazel_${USER}" ]; then
  OUTPUT_ROOT="${HOME}/.cache/bazel/_bazel_${USER}"
elif [ -d "/private/var/tmp/_bazel_${USER}" ]; then
  OUTPUT_ROOT="/private/var/tmp/_bazel_${USER}"
else
  # the known places for output_bases don't exist so just let bazel figure it out.
  return
fi

# If we haven't enabled multi-bases, just run bazel normally.
if [ ! -d "${OUTPUT_ROOT}/bases" ]; then 
  return
fi

OUTPUT_BASES="${OUTPUT_ROOT}/bases/${SUFFIX}"


# Make creating the claim pidfile atomic.
set -o noclobber

# Find and claim an available base.
for i in {1..4}; do
  BASE="${OUTPUT_BASES}/${i}"
  mkdir -p "${BASE}"
  PIDFILE="${BASE}.inuse"

  # Check for and move orphaned PID files out of the way.
  if [ -f "$PIDFILE" ]; then
    OWNER="$(cat ${PIDFILE} 2>/dev/null)"
    if ! ps -p "${OWNER}" >/dev/null 2>/dev/null && [ "${OWNER}" = "$(cat ${PIDFILE} 2>/dev/null)" ]; then
      # move the file to dest name picked based on the owner so it will fail if 
      # another process has already performed this specific orphan cleanup.
      mv -n "${PIDFILE}" "${PIDFILE}.orphan-${OWNER}" 2>/dev/null
    fi
  fi
  
  # Claim and use this base if able (atomic thanks to noclobber above).
  if echo "$$" 2> /dev/null > "${PIDFILE}"; then
    trap "rm \"$PIDFILE\"" EXIT
    export BAZEL_OUTPUT_BASE="${BASE}"
    break
  fi
done
