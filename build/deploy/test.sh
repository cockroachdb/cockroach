#!/bin/sh
# This script is placed inside of the deploy container in /test/.
# It is called by the entry point of the container and will execute
# all statically linked Go tests in its subdirectories.
#
# This is used by mkimage.sh to run the tests (which are for that
# purpose mounted to /test/.out.
#
# Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

set -eu

BUILD="../build"
mkdir -p "${BUILD}"
for f in $(find "${BUILD}" -name '*.test' -type f); do
  >&2 echo executing "$f" && ./$f || exit $?;
done
