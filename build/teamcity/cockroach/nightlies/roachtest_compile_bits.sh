#!/usr/bin/env bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail

# N.B. `$root` is defined in build/teamcity-support.sh;
# must run it first, when this script is used outside of roachtest_nightly_impl.sh

source $root/build/teamcity/util/roachtest_arch_util.sh

if [ "$#" -eq 0 ]; then
  echo "Builds all bits needed for roachtests and stages them in bin/ and lib/."
  echo ""
  echo "Usage: $0 [--with-code-coverage] arch [arch...]"
  echo "  where arch is one of: amd64, arm64, amd64-fips"
  exit 1
fi

os=linux

components=()
extra_flags=""

for arg in "$@"; do
  case "$arg" in
    --with-code-coverage)
      extra_flags="$arg"
      ;;
    *)
      # Fail now if the argument is not a valid arch.
      arch_to_config $arg >/dev/null || exit 1
      components+=($os/$arg/cockroach)
      components+=($os/$arg/cockroach-ea)
      components+=($os/$arg/workload)
      components+=($os/$arg/libgeos)
      ;;
  esac
done

# We need to build roachtest and geos libraries (necessary for local tests) for
# the host architecture.
host_arch=$(get_host_arch)
echo "Host architecture: $host_arch"
components+=($os/$host_arch/roachtest)
components+=($os/$host_arch/roachprod)
components+=($os/$host_arch/libgeos)

# Prepare the bin/ and lib/ directories.
mkdir -p bin lib
chmod o+rwx bin lib

# Sort and dedup components (libgeos can show up twice).
for comp in $(printf "%s\n" "${components[@]}" | sort -u); do
  "$(dirname $0)"/roachtest_compile_component.sh $extra_flags $comp
done

cp -p bin/roachtest.$os-$host_arch bin/roachtest
cp -p bin/roachprod.$os-$host_arch bin/roachprod
# N.B. geos does not support the architecture suffix (see getLibraryExt() in
# geos.go).
cp -p lib/libgeos.$os-$host_arch.so lib/libgeos.so
cp -p lib/libgeos_c.$os-$host_arch.so lib/libgeos_c.so

ls -l bin
ls -l lib
