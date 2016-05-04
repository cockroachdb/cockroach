#! /bin/sh

# This script produces the list of authors for a given source file,
# listed in decreasing order of the number of commits by that author
# that have touched the file.

file=${1:?}

git shortlog -e "$file" \
    | grep -v '^ ' \
    | grep -v '^$' \
    | sed -e 's/\(.*\)(\(.*\)):/\2 \1/g' \
    | sort -nr \
    | sed -e 's,^[^ ]* ,// Author: ,g'
