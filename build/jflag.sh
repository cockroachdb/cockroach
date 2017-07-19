#!/usr/bin/env bash

set -euo pipefail

# Try to determine the number of available cores and output as a -j flag for
# make.

# Determine the flags of the parent process (make).
parentflags=$(ps -o args= $(ps -o ppid= -p $$))

# If the parent was called with -j, don't output a flag.
if [[ $parentflags != *"-j"* ]]; then
  # Now try to find the number of cores.
  # nproc works on Linux with coreutils.
  # sysctl -n hw.ncpu works on mac.
  # Grepping /proc/cpuinfo works on other Linuxes.
  # Otherwise give up and just use a single process.
  ncpus=$(nproc 2>/dev/null || \
          sysctl -n hw.ncpu 2>/dev/null || \
          grep -c processor /proc/cpuinfo 2>/dev/null || \
          (echo "1" && echo "Can't determine number of cores, using no parallelism" 1>&2))

  echo "-j${ncpus}"
  echo "Running make with -j${ncpus}" 1>&2
fi
