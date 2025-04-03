#!/usr/bin/env bash

# Copyright 2022 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


# This is used through bazel when generating sql.go, plpgsql.go, and jsonpath.go.
# Look at BUILD.bazel in pkg/sql/parser, pkg/sql/plpgsql/parser, or
# pkg/util/jsonpath/parser for usage.

set -euo pipefail

LANG=$2
SYMUNION="${LANG}"'SymUnion'
GENYACC=$LANG-gen.y


    awk -v regex="$SYMUNION" '/func.*'"$SYMUNION"'/ {print $(NF - 1)}' $1 | \
        sed -e 's/[]\/$*.^|[]/\\&/g' | \
        sed -e "s/^/s_(type|token) <(/" | \
        awk '{print $0")>_\\1 <union> /* <\\2> */_"}' > types_regex.tmp

    sed -E -f types_regex.tmp < $1 | \
        if [ $LANG != plpgsql ] && [ $LANG != pgrepl ] && [ $LANG != jsonpath ]; then \
            awk -f $3 | \
          sed -Ee 's,//.*$$,,g;s,/[*]([^*]|[*][^/])*[*]/, ,g;s/ +$$//g' > $GENYACC
        else
          sed -Ee 's,//.*$$,,g;s,/[*]([^*]|[*][^/])*[*]/, ,g;s/ +$$//g' > $GENYACC
        fi;

    rm types_regex.tmp

    ret=$($5 -p $LANG -o $4 $GENYACC); \
      if expr "$ret" : ".*conflicts" >/dev/null; then \
        echo "$ret"; exit 1; \
      fi;
    rm $GENYACC
