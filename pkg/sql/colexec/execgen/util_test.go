// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execgen

import (
	"fmt"

	"github.com/dave/dst"
	"github.com/dave/dst/decorator"
)

func parseStmts(stmts string) []dst.Stmt {
	inputStr := fmt.Sprintf(`package main
func test() {
  %s
}`, stmts)
	f, err := decorator.Parse(inputStr)
	if err != nil {
		panic(err)
	}
	return f.Decls[0].(*dst.FuncDecl).Body.List
}

func parseDecls(decls string) []dst.Decl {
	inputStr := fmt.Sprintf(`package main
%s
`, decls)
	f, err := decorator.Parse(inputStr)
	if err != nil {
		panic(err)
	}
	return f.Decls
}
