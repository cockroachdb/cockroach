#!/usr/bin/env bash

# This is used through bazel when generating sql.go and plpgsql.go.
# Look at BUILD.bazel in pkg/sql/parser or pkg/plpgsql/parser for
# usage.

set -euo pipefail

if [ "$6" != plpgsql ]; then
  SYMUNION="sqlSymUnion"
  GENYACC=sql-gen.y
else
  SYMUNION="plpgsqlSymUnion"
  GENYACC=plpgsql-gen.y
fi;

    awk -v regex="$SYMUNION" '/func.*'"$SYMUNION"'/ {print $(NF - 1)}' $1 | \
        sed -e 's/[]\/$*.^|[]/\\&/g' | \
        sed -e "s/^/s_(type|token) <(/" | \
        awk '{print $0")>_\\1 <union> /* <\\2> */_"}' > types_regex.tmp

    sed -E -f types_regex.tmp < $1 | \
        if [ "$6" != plpgsql ]; then \
            awk -f $2 | \
          sed -Ee 's,//.*$$,,g;s,/[*]([^*]|[*][^/])*[*]/, ,g;s/ +$$//g' > $GENYACC
        else
          sed -Ee 's,//.*$$,,g;s,/[*]([^*]|[*][^/])*[*]/, ,g;s/ +$$//g' > $GENYACC
        fi;

    rm types_regex.tmp

    ret=$($4 -p $6 -o $3 $GENYACC); \
      if expr "$ret" : ".*conflicts" >/dev/null; then \
        echo "$ret"; exit 1; \
      fi;
    rm $GENYACC
    $5 -w $3
