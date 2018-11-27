#!/bin/sh

# This script produces the list of authors that are "git blame"d for lines in
# .go files under the current directory.
#
# The regular expression for filenames can be overridden on the command line.
# To include all files, use a dot:
#   git-blame-stats.sh .

FILE_REGEXP='\.go$'
if [ -n "$1" ]; then FILE_REGEXP=$1; fi

toplevel=$(git rev-parse --show-toplevel)
cp ${toplevel}/scripts/.mailmap ${toplevel}/.mailmap

git ls-tree -r HEAD \
  | cut -f 2 \
  | grep -E "$FILE_REGEXP" \
  | xargs -n1 git blame --line-porcelain \
  | grep "author " \
  | sort \
  | uniq -c \
  | sort -nr

rm ${toplevel}/.mailmap
