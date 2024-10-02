// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgreplparser

import (
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

func Parse(sql string) (statements.Statement[tree.Statement], error) {
	lexer := newLexer(sql)
	p := pgreplNewParser()
	if p.Parse(lexer) != 0 {
		if lexer.lastError == nil {
			return statements.Statement[tree.Statement]{}, errors.AssertionFailedf("expected lexer error but got none")
		}
		return statements.Statement[tree.Statement]{}, lexer.lastError
	}
	if lexer.stmt == nil {
		return statements.Statement[tree.Statement]{}, errors.AssertionFailedf("expected statement but got none")
	}
	return statements.Statement[tree.Statement]{
		AST: lexer.stmt,
		SQL: sql,
	}, nil
}
