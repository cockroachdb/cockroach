#!/bin/bash

set -e

PACKAGE="$1"
TYPE="$2"
INPUT="$3"
OUTPUT="$4"

TEMP=$(tempfile -d .)

cleanup() {
  rm "$TEMP"
}

trap cleanup EXIT

cat > "$TEMP" <<- EOF
// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package $1

import "context"

EOF

cat "$INPUT" | awk >> "$TEMP" -v TYPE="$TYPE" '
BEGIN { for (i in ops) {} };
/type [A-Z].* struct {/ {
    ops[length(ops)] = $2;
};
END {
    printf( \
"// %sOp is an operation which can be visited by %sVisitor\n" \
"type %sOp interface {\n" \
"\tOp\n" \
"\tVisit(context.Context, %sVisitor) error\n" \
"}\n\n", TYPE, TYPE, TYPE, TYPE);
    printf( \
"// %sVisitor is a visitor for %sOp operations.\n" \
"type %sVisitor interface {\n", \
TYPE, TYPE, TYPE);
    for (i in ops) {
        printf("\t%s(context.Context, %s) error\n", ops[i], ops[i]);
    }
    printf("}\n");
    for (i in ops) {
        printf(\
"\n// Visit is part of the %sOp interface.\n" \
"func (op %s) Visit(ctx context.Context, v %sVisitor) error {\n" \
"\treturn v.%s(ctx, op)\n" \
"}\n", TYPE, ops[i], TYPE, ops[i]);
    }
}'

cat "$TEMP" | crlfmt -tab 2 > "$OUTPUT"
