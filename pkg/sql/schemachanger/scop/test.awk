
BEGIN {
   printf( \
"// Copyright %s The Cockroach Authors.\n" \
"//\n" \
"// Use of this software is governed by the Business Source License\n" \
"// included in the file licenses/BSL.txt.\n" \
"//\n" \
"// As of the Change Date specified in that file, in accordance with\n" \
"// the Business Source License, use of this software will be governed\n" \
"// by the Apache License, Version 2.0, included in the file\n" \
"// licenses/APL.txt.\n\n" \
"\npackage %s\n\n" \
"import \"context\"\n\n" \
, YEAR, PACKAGE);
  for (i in ops) {}; # declare ops as an array
}
/type [A-Z].* struct {/ {
    ops[length(ops)] = $2;
}
END {
    printf( \
"// %sVisitor is a visitor for %sOp operations.\n" \
"type %sVisitor interface {\n", \
TYPE, TYPE, TYPE);
    for (i in ops) {
        printf("\t%s(context.Context, %sVisitor) error\n", ops[i], TYPE);
    }
    printf("}\n\n");
    for (i in ops) {
        printf(\
"// Visit is part of the %sOp interface.\n" \
"func (op %s) Visit(ctx context.Context, v %sVisitor) error {\n" \
"\treturn v.%s(ctx, op)\n" \
"}\n\n", TYPE, ops[i], TYPE, ops[i]);
    }
}
