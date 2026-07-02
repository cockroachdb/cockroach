#!/usr/bin/env bash

# Copyright 2021 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euo pipefail

# N.B. `$root` is defined in build/teamcity-support.sh;
# must run it first, when this script is used outside of roachtest_nightly_impl.sh

source $root/build/teamcity/util/roachtest_arch_util.sh

if [[ "${ROACHTEST_BUILD_CACHE:-}" == "true" ]]; then
  gcs_setup_credentials
fi

if [ "$#" -eq 0 ]; then
  echo "Builds all bits needed for roachtests and stages them in bin/ and lib/."
  echo ""
  echo "Usage: $0 [--with-code-coverage] [--skip-host-tooling-unless-native-arch] arch [arch...]"
  echo "  where arch is one of: amd64, arm64, amd64-fips"
  echo "  --skip-host-tooling-unless-native-arch omits the host-architecture roachtest/roachprod/libgeos"
  echo "    binaries (needed only to run tests locally, not to warm the cache),"
  echo "    unless the host architecture is itself one of the requested targets"
  exit 1
fi

os=linux

components=()
requested_arches=()
extra_flags=""
skip_host_tooling_unless_native_arch=false

for arg in "$@"; do
  case "$arg" in
    --with-code-coverage)
      extra_flags="$arg"
      ;;
    --skip-host-tooling-unless-native-arch)
      skip_host_tooling_unless_native_arch=true
      ;;
    *)
      # Fail now if the argument is not a valid arch.
      arch_to_config $arg >/dev/null || exit 1
      requested_arches+=($arg)
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

# --skip-host-tooling-unless-native-arch lets cache-warming callers (which never run tests) avoid
# every per-arch build redundantly recompiling the host tooling. We still build
# it when the host architecture is itself one of the requested targets: that
# build is already compiling host-arch bits, so adding roachtest/roachprod is
# cheap and keeps the host tooling in the cache for the test jobs to reuse. In
# the split-by-arch setup this means exactly one build (the host-arch one) warms
# the host tooling, and the cross-arch builds skip it.
build_host_tooling=true
if [[ "$skip_host_tooling_unless_native_arch" == "true" ]]; then
  build_host_tooling=false
  for arch in ${requested_arches[@]+"${requested_arches[@]}"}; do
    if [[ "$arch" == "$host_arch" ]]; then
      build_host_tooling=true
      break
    fi
  done
fi

if [[ "$build_host_tooling" == "true" ]]; then
  components+=($os/$host_arch/roachtest)
  components+=($os/$host_arch/roachprod)
  components+=($os/$host_arch/libgeos)
else
  echo "Skipping host-architecture tooling (roachtest, roachprod, libgeos);" \
       "host arch $host_arch is not among the requested targets."
fi

# Prepare the bin/ and lib/ directories.
mkdir -p bin lib
chmod o+rwx bin lib

# Sort and dedup components (libgeos can show up twice).
for comp in $(printf "%s\n" "${components[@]}" | sort -u); do
  "$(dirname $0)"/roachtest_compile_component.sh $extra_flags $comp
done

# These canonical (arch-suffixless) copies exist only so local test runs find
# the host binaries; skip them when host tooling was not built.
if [[ "$build_host_tooling" == "true" ]]; then
  cp -p bin/roachtest.$os-$host_arch bin/roachtest
  cp -p bin/roachprod.$os-$host_arch bin/roachprod
  # N.B. geos does not support the architecture suffix (see getLibraryExt() in
  # geos.go).
  cp -p lib/libgeos.$os-$host_arch.so lib/libgeos.so
  cp -p lib/libgeos_c.$os-$host_arch.so lib/libgeos_c.so
fi

ls -l bin
ls -l lib
