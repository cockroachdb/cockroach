#!/bin/sh
# This script is placed inside of the deploy container in /test/.
# It is called by the entry point of the container and will execute
# all statically linked Go tests in its subdirectories.
#
# This is used by mkimage.sh to run the tests (which are for that
# purpose mounted to /test/.out.
#
# Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)
mkdir -p .out
for f in $(find .out -name '*.test' -type f); do
  ldd "$f" > /dev/null 2>&1
  if [ $? -ne 0 ]; then
    >&2 echo "skipping '$f' (not statically linked)"
    continue
  fi
  >&2 echo executing "$f" && (cd -P $(dirname $f) && ./$(basename $f)) || exit $?;
done 
