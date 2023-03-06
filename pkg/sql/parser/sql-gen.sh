#!/usr/bin/env bash

# This is used through bazel when generating sql.go and plpgsql.go.
# Look at BUILD.bazel in pkg/sql/parser or pkg/plpgsql/parser for
# usage.

set -euo pipefail


if [ "$#" -eq 5 ]; then
    awk '/func.*sqlSymUnion/ {print $(NF - 1)}' $1 | \
        sed -e 's/[]\/$*.^|[]/\\&/g' | \
        sed -e "s/^/s_(type|token) <(/" | \
        awk '{print $0")>_\\1 <union> /* <\\2> */_"}' > types_regex.tmp

    sed -E -f types_regex.tmp < $1 | \
          awk -f $2 | \
        sed -Ee 's,//.*$$,,g;s,/[*]([^*]|[*][^/])*[*]/, ,g;s/ +$$//g' > sql-gen.y
    rm types_regex.tmp

    ret=$($4 -p sql -o $3 sql-gen.y); \
      if expr "$ret" : ".*conflicts" >/dev/null; then \
        echo "$ret"; exit 1; \
      fi;
    rm sql-gen.y
    $5 -w $3
else
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
fi
