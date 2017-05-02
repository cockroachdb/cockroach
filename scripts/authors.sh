#! /bin/sh

# This script produces the list of authors for a given source file,
# listed in decreasing order of the number of commits by that author
# that have touched the file.

file=${1:?}

git log --no-merges --format='%aN <%aE>' "$file" \
    | sort \
    | uniq -c \
    | sort -nr \
    | sed -e 's/^ *//g' \
    | cut -d' ' -f2- \
    | sed -e 's,^,// Author: ,g'
