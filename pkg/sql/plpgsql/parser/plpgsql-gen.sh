#!/usr/bin/env bash

# This is used through bazel when generating plpgsql.go. Look at BUILD.bazel for
# usage.

set -euo pipefail

awk '/func.*plpgsqlSymUnion/ {print $(NF - 1)}' $1 | \
    sed -e 's/[]\/$*.^|[]/\\&/g' | \
    sed -e "s/^/s_(type|token) <(/" | \
    awk '{print $0")>_\\1 <union> /* <\\2> */_"}' > types_regex.tmp

sed -E -f types_regex.tmp < $1 | \
    sed -Ee 's,//.*$$,,g;s,/[*]([^*]|[*][^/])*[*]/, ,g;s/ +$$//g' > plpgsql-gen.y
rm types_regex.tmp

ret=$($3 -p plpgsql -o $2 plpgsql-gen.y); \
  if expr "$ret" : ".*conflicts" >/dev/null; then \
    echo "$ret"; exit 1; \
  fi;
rm plpgsql-gen.y
$4 -w $2
