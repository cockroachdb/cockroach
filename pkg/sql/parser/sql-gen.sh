#!/usr/bin/env bash

# This is used through bazel when generating sql.go. Look at BUILD.bazel for
# usage.

set -euo pipefail

awk '/func.*sqlSymUnion/ {print $(NF - 1)}' $1 | \
    sed -e 's/[]\/$*.^|[]/\\&/g' | \
    sed -e "s/^/s_(type|token) <(/" | \
    awk '{print $0")>_\\1 <union> /* <\\2> */_"}' > types_regex.tmp

sed -E -f types_regex.tmp < $1 | \
    awk -f $2 | \
    sed -Ee 's,//.*$$,,g;s,/[*]([^*]|[*][^/])*[*]/, ,g;s/ +$$//g' > sql-gen.y
rm types_regex.tmp

ret=$($4 -p sql -o $3 sql-gen.y); \
  if expr "$$ret" : ".*conflicts" >/dev/null; then \
    echo "$$ret"; exit 1; \
  fi;
rm sql-gen.y
$5 -w $3
