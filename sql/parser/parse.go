// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

// This code was derived from https://github.com/youtube/vitess.
//
// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

package parser

import (
	"bytes"
	"errors"

	"github.com/cockroachdb/cockroach/util"
)

//go:generate make

// StatementList is a list of statements.
type StatementList []Statement

func (l StatementList) String() string {
	var buf bytes.Buffer
	for i, s := range l {
		if i > 0 {
			buf.WriteString("; ")
		}
		buf.WriteString(s.String())
	}
	return buf.String()
}

// Syntax is an enum of the various syntax types.
type Syntax int

//go:generate stringer -type=Syntax
const (
	// Implicit default, must stay in the zero-value position.
	Traditional Syntax = iota
	Modern
)

// Parser wraps a scanner, parser and other utilities present in the parser
// package.
type Parser struct {
	scanner          scanner
	parserImpl       sqlParserImpl
	normalizeVisitor normalizeVisitor
}

// Parse parses the sql and returns a list of statements.
func (p *Parser) Parse(sql string, syntax Syntax) (stmts StatementList, err error) {
	defer func() {
		switch r := recover(); r {
		case nil:
		case errUnimplemented:
			err = errUnimplemented
		default:
			panic(r)
		}
	}()
	p.scanner.init(sql, syntax)
	if p.parserImpl.Parse(&p.scanner) != 0 {
		return nil, errors.New(p.scanner.lastError)
	}
	return p.scanner.stmts, nil
}

// NormalizeExpr is wrapper around ctx.NormalizeExpr which avoids allocation of
// a normalizeVisitor.
func (p *Parser) NormalizeExpr(ctx EvalContext, expr Expr) (Expr, error) {
	p.normalizeVisitor = normalizeVisitor{ctx: ctx}
	expr, _ = WalkExpr(&p.normalizeVisitor, expr)
	return expr, p.normalizeVisitor.err
}

// parse parses the sql and returns a list of statements.
func parse(sql string, syntax Syntax) (StatementList, error) {
	var p Parser
	return p.Parse(sql, syntax)
}

// parseTraditional is short-hand for parse(sql, Traditional)
func parseTraditional(sql string) (StatementList, error) {
	return parse(sql, Traditional)
}

// ParseOne parses a sql statement.
func ParseOne(sql string, syntax Syntax) (Statement, error) {
	stmts, err := parse(sql, syntax)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, util.Errorf("expected 1 statement, but found %d", len(stmts))
	}
	return stmts[0], nil
}

// ParseOneTraditional is short-hand for ParseOne(sql, Traditional)
func ParseOneTraditional(sql string) (Statement, error) {
	return ParseOne(sql, Traditional)
}

// parseExpr parses a sql expression.
func parseExpr(expr string, syntax Syntax) (Expr, error) {
	stmt, err := ParseOne(`SELECT `+expr, syntax)
	if err != nil {
		return nil, err
	}
	sel, ok := stmt.(*Select)
	if !ok {
		return nil, util.Errorf("expected a SELECT statement, but found %T", stmt)
	}
	selClause, ok := sel.Select.(*SelectClause)
	if !ok {
		return nil, util.Errorf("expected a SELECT statement, but found %T", sel.Select)
	}
	if n := len(selClause.Exprs); n != 1 {
		return nil, util.Errorf("expected 1 expression, but found %d", n)
	}
	return selClause.Exprs[0].Expr, nil
}

// ParseExprTraditional is a short-hand for parseExpr(sql, Traditional)
func ParseExprTraditional(sql string) (Expr, error) {
	return parseExpr(sql, Traditional)
}
