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
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/util"
	"github.com/pkg/errors"
)

//go:generate make

// StatementList is a list of statements.
type StatementList []Statement

// Format implements the NodeFormatter interface.
func (l StatementList) Format(buf *bytes.Buffer, f FmtFlags) {
	for i, s := range l {
		if i > 0 {
			buf.WriteString("; ")
		}
		FormatNode(buf, f, s)
	}
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
	scanner            Scanner
	parserImpl         sqlParserImpl
	normalizeVisitor   normalizeVisitor
	isAggregateVisitor IsAggregateVisitor
}

// Parse parses the sql and returns a list of statements.
func (p *Parser) Parse(sql string, syntax Syntax) (stmts StatementList, err error) {
	defer func() {
		if r := recover(); r != nil {
			// Panic on anything except unimplemented errors.
			if _, ok := r.(util.UnimplementedWithIssueError); !ok && r != errUnimplemented {
				panic(r)
			}
			err = r.(error)
		}
	}()
	p.scanner.init(sql, syntax)
	if p.parserImpl.Parse(&p.scanner) != 0 {
		return nil, errors.New(p.scanner.lastError)
	}
	return p.scanner.stmts, nil
}

// NoTypePreference can be provided to TypeCheck's desired type parameter to indicate that
// the caller of the function has no preference on the type of the resulting TypedExpr.
var NoTypePreference = Datum(nil)

// TypeCheck performs type checking on the provided expression tree, returning
// the new typed expression tree, which additionally permits evaluation and type
// introspection globally and on each sub-tree.
//
// While doing so, it will fold numeric constants and bind placeholder names to
// their inferred types in the provided context. The optional desired parameter can
// be used to hint the desired type for the root of the resulting typed expression
// tree.
func TypeCheck(expr Expr, ctx *SemaContext, desired Datum) (TypedExpr, error) {
	expr, err := foldConstantLiterals(expr)
	if err != nil {
		return nil, err
	}
	return expr.TypeCheck(ctx, desired)
}

// TypeCheckAndRequire performs type checking on the provided expression tree in
// an identical manner to TypeCheck. It then asserts that the resulting TypedExpr
// has the provided return type, returning both the typed expression and an error
// if it does not.
func TypeCheckAndRequire(expr Expr, ctx *SemaContext, required Datum, op string) (TypedExpr, error) {
	typedExpr, err := TypeCheck(expr, ctx, required)
	if err != nil {
		return nil, err
	}
	if typ := typedExpr.ReturnType(); !(typ.TypeEqual(required) || typ == DNull) {
		return typedExpr, fmt.Errorf("argument of %s must be type %s, not type %s",
			op, required.Type(), typ.Type())
	}
	return typedExpr, nil
}

// NormalizeExpr is wrapper around ctx.NormalizeExpr which avoids allocation of
// a normalizeVisitor.
func (p *Parser) NormalizeExpr(ctx *EvalContext, typedExpr TypedExpr) (TypedExpr, error) {
	if ctx.SkipNormalize {
		return typedExpr, nil
	}
	p.normalizeVisitor = normalizeVisitor{ctx: ctx}
	expr, _ := WalkExpr(&p.normalizeVisitor, typedExpr)
	if err := p.normalizeVisitor.err; err != nil {
		return nil, err
	}
	return expr.(TypedExpr), nil
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
		return nil, errors.Errorf("expected 1 statement, but found %d", len(stmts))
	}
	return stmts[0], nil
}

// ParseOneTraditional is short-hand for ParseOne(sql, Traditional)
func ParseOneTraditional(sql string) (Statement, error) {
	return ParseOne(sql, Traditional)
}

// parseExprs parses one or more sql expression.
func parseExprs(exprs []string, syntax Syntax) (Exprs, error) {
	stmt, err := ParseOne(fmt.Sprintf("SET ROW (%s)", strings.Join(exprs, ",")), syntax)
	if err != nil {
		return nil, err
	}
	set, ok := stmt.(*Set)
	if !ok {
		return nil, errors.Errorf("expected a SET statement, but found %T", stmt)
	}
	return set.Values, nil
}

// ParseExprsTraditional is a short-hand for parseExprs(Traditional, sql)
func ParseExprsTraditional(sql []string) (Exprs, error) {
	if len(sql) == 0 {
		return Exprs{}, nil
	}
	return parseExprs(sql, Traditional)
}

// ParseExprTraditional is a short-hand for parseExprs(Traditional, []string{sql})
func ParseExprTraditional(sql string) (Expr, error) {
	exprs, err := parseExprs([]string{sql}, Traditional)
	if err != nil {
		return nil, err
	}
	if len(exprs) != 1 {
		return nil, errors.Errorf("expected 1 expression, found %d", len(exprs))
	}
	return exprs[0], nil
}
