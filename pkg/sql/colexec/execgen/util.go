// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execgen

import (
	"fmt"
	"strings"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
)

func prettyPrintStmts(stmts ...dst.Stmt) string {
	if len(stmts) == 0 {
		return ""
	}
	f := &dst.File{
		Name: dst.NewIdent("main"),
		Decls: []dst.Decl{
			&dst.FuncDecl{
				Name: dst.NewIdent("test"),
				Type: &dst.FuncType{},
				Body: &dst.BlockStmt{
					List: stmts,
				},
			},
		},
	}
	var ret strings.Builder
	_ = decorator.Fprint(&ret, f)
	prelude := `package main

func test() {
`
	postlude := `}
`
	s := ret.String()
	return strings.TrimSpace(s[len(prelude) : len(s)-len(postlude)])
}

func prettyPrintExprs(exprs ...dst.Expr) string {
	stmts := make([]dst.Stmt, len(exprs))
	for i := range exprs {
		stmts[i] = &dst.ExprStmt{X: exprs[i]}
	}
	return prettyPrintStmts(stmts...)
}

func parseStmt(stmt string) (dst.Stmt, error) {
	f, err := decorator.Parse(fmt.Sprintf(
		`package main
func test() {
	%s
}`, stmt))
	if err != nil {
		return nil, err
	}
	return f.Decls[0].(*dst.FuncDecl).Body.List[0], nil
}

func mustParseStmt(stmt string) dst.Stmt {
	ret, err := parseStmt(stmt)
	if err != nil {
		panic(err)
	}
	return ret
}
