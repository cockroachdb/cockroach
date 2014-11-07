#!/bin/sh
for f in $(find .out -name '*.test' -type f); do
  ldd "$f" > /dev/null 2>&1
  if [ $? -ne 0 ]; then
    >&2 echo "skipping '$f' (not statically linked)"
    continue
  fi
  >&2 echo executing "$f" && (cd -P $(dirname $f) && ./$(basename $f)) || exit $?;
done 
