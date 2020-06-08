// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
