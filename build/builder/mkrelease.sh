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
#   - arm64-linux-gnu:      arm64, Linux 3.7.10, dynamically link glibc 2.12.2
#   - amd64-darwin:         amd64, macOS 10.9
#   - amd64-windows:        amd64, Windows 8, statically link all non-Windows libraries
#   - s390x-linux-gnu:      s390x, Linux 2.6.32, dynamically link glibc 2.12.2
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

# Callers can set the MKRELEASE_BUILDTYPE environment variable to configure a
# custom build type.
BUILDTYPE="${MKRELEASE_BUILDTYPE:-release}"

case "${1-}" in
  ""|?(amd64-)linux?(-gnu))
    args=(
      XGOOS=linux
      XGOARCH=amd64
      XCMAKE_SYSTEM_NAME=Linux
      TARGET_TRIPLE=x86_64-unknown-linux-gnu
      # -lrt is needed as clock_gettime isn't part of glibc prior to 2.17.
      # If we update to a newer glibc, the -lrt can be removed.
      LDFLAGS="-static-libgcc -static-libstdc++ -lrt"
      SUFFIX=-linux-2.6.32-gnu-amd64
    ) ;;

  ?(arm64-)linux?(-gnu))
    # Manually set the correct values for configure checks that libkrb5 won't be
    # able to perform because we're cross-compiling.
    export krb5_cv_attr_constructor_destructor=yes
    export ac_cv_func_regcomp=yes
    export ac_cv_printf_positional=yes
    args=(
      XGOOS=linux
      XGOARCH=arm64
      XCMAKE_SYSTEM_NAME=Linux
      TARGET_TRIPLE=aarch64-unknown-linux-gnu
      LDFLAGS="-static-libgcc -static-libstdc++"
      SUFFIX=-linux-3.7.10-gnu-aarch64
    ) ;;

  ?(amd64-)darwin)
    args=(
      XGOOS=darwin
      XGOARCH=amd64
      XCMAKE_SYSTEM_NAME=Darwin
      TARGET_TRIPLE=x86_64-apple-darwin19
      EXTRA_XCMAKE_FLAGS=-DCMAKE_INSTALL_NAME_TOOL=x86_64-apple-darwin19-install_name_tool
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

  ?(s390x-)linux?(-gnu))
    # Manually set the correct values for configure checks that libkrb5 won't be
    # able to perform because we're cross-compiling.
    export krb5_cv_attr_constructor_destructor=yes
    export ac_cv_func_regcomp=yes
    export ac_cv_printf_positional=yes
    args=(
      XGOOS=linux
      XGOARCH=s390x
      XCMAKE_SYSTEM_NAME=Linux
      TARGET_TRIPLE=s390x-ibm-linux-gnu
      # -lrt is needed as clock_gettime isn't part of glibc prior to 2.17.
      # If we update to a newer glibc, the -lrt can be removed.
      LDFLAGS="-static-libgcc -static-libstdc++ -lrt"
      SUFFIX=-linux-2.6.32-gnu-s390x
    ) ;;
  *)  die "unknown release configuration: $1" ;;
esac

if [ $# -ge 1 ]; then
    shift
fi

(set -x && CGO_ENABLED=1 make BUILDTYPE=$BUILDTYPE "${args[@]}" "$@")
