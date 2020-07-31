#!/usr/bin/env bash

# This script builds a CockroachDB release binary, potentially cross compiling
# for a different platform. It must be run in the cockroachdb/builder docker
# image, as it depends on cross-compilation toolchains available there. Usage:
#
#   mkrelease [CONFIGURATION] [MAKE-GOALS...]
#
# Possible configurations:
#
#   - amd64-linux-gnu:      amd64, Linux 2.6.32, dynamically link glibc 2.12.2
#   - amd64-linux-musl:     amd64, Linux 2.6.32, statically link musl 1.1.16
#   - amd64-linux-msan:     amd64, recent Linux, enable Clang's memory sanitizer
#   - arm64-linux-gnueabi:  arm64, Linux 3.7.10, dynamically link glibc 2.12.2
#   - amd64-darwin:         amd64, macOS 10.9
#   - amd64-windows:        amd64, Windows 8, statically link all non-Windows libraries
#
# When specifying configurations on the command line, the architecture prefix
# and/or the ABI suffix can be omitted, in which case a suitable default will
# be selected. If no release arguments are specified, the configuration
# amd64-linux-gnu is used (this is the default linux binary).
#
# In order to specify MAKE-GOALS, a configuration must be explicitly
# specified.
#
# Note to maintainers: these configurations must be kept in sync with the
# crosstool-ng toolchains installed in the Dockerfile.

set -euo pipefail
shopt -s extglob

cd "$(dirname "$(readlink -f "$0")")/../.."
source build/shlib.sh

case "${1-}" in
  ""|?(amd64-)linux?(-gnu))
    args=(
      XGOOS=linux
      XGOARCH=amd64
      XCMAKE_SYSTEM_NAME=Linux
      TARGET_TRIPLE=x86_64-unknown-linux-gnu
# -lrt is needed as clock_gettime isn't part of glibc prior to 2.17
# once we update - the -lrt can be removed
      LDFLAGS="-static-libgcc -static-libstdc++ -lrt"
      SUFFIX=-linux-2.6.32-gnu-amd64
    ) ;;

  ?(amd64-)linux-musl)
    args=(
      XGOOS=linux
      XGOARCH=amd64
      XCMAKE_SYSTEM_NAME=Linux
      TARGET_TRIPLE=x86_64-unknown-linux-musl
      LDFLAGS=-static
      SUFFIX=-linux-2.6.32-musl-amd64
    ) ;;

  ?(arm64-)linux?(-gnueabi))
    args=(
      XGOOS=linux
      XGOARCH=arm64
      XCMAKE_SYSTEM_NAME=Linux
      TARGET_TRIPLE=aarch64-unknown-linux-gnueabi
      LDFLAGS="-static-libgcc -static-libstdc++"
      SUFFIX=-linux-3.7.10-gnu-aarch64
    ) ;;

  ?(amd64-)linux-msan)
    flags="-fsanitize=memory -fsanitize-memory-track-origins -fno-omit-frame-pointer -I/libcxx_msan/include -I/libcxx_msan/include/c++/v1"
    args=(
      CFLAGS="$flags"
      CXXFLAGS="$flags"
      LDFLAGS="-fsanitize=memory -stdlib=libc++ -L/libcxx_msan/lib -lc++abi -Wl,-rpath,/libcxx_msan/lib"
      GOFLAGS=-msan
      TAGS=stdmalloc
    ) ;;

  ?(amd64-)darwin)
    args=(
      XGOOS=darwin
      XGOARCH=amd64
      XCMAKE_SYSTEM_NAME=Darwin
      TARGET_TRIPLE=x86_64-apple-darwin14
      EXTRA_XCMAKE_FLAGS=-DCMAKE_INSTALL_NAME_TOOL=x86_64-apple-darwin14-install_name_tool
      SUFFIX=-darwin-10.10-amd64
    ) ;;

  ?(amd64-)windows)
    args=(
      XGOOS=windows
      XGOARCH=amd64
      XCMAKE_SYSTEM_NAME=Windows
      TARGET_TRIPLE=x86_64-w64-mingw32
      LDFLAGS=-static
      SUFFIX=-windows-6.2-amd64
    ) ;;

  *)  die "unknown release configuration: $1" ;;
esac

if [ $# -ge 1 ]; then
    shift
fi

# lib is populated in v20.2 or higher, but we make a placeholder for the same
# files in /lib such that TeamCity can pick up the artifacts.
# These placeholders are unused in the actual release process.
(set -x && mkdir -p lib && touch lib/libgeos.so && touch lib/libgeos_c.so && CGO_ENABLED=1 make BUILDTYPE=release "${args[@]}" "$@")
