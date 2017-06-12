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

// Parser wraps a scanner, parser and other utilities present in the parser
// package.
type Parser struct {
	scanner               Scanner
	parserImpl            sqlParserImpl
	normalizeVisitor      normalizeVisitor
	isAggregateVisitor    IsAggregateVisitor
	containsWindowVisitor ContainsWindowVisitor
}

// Parse parses the sql and returns a list of statements.
func (p *Parser) Parse(sql string) (stmts StatementList, err error) {
	p.scanner.init(sql)
	if p.parserImpl.Parse(&p.scanner) != 0 {
		return nil, errors.New(p.scanner.lastError)
	}
	return p.scanner.stmts, nil
}

// TypeCheck performs type checking on the provided expression tree, returning
// the new typed expression tree, which additionally permits evaluation and type
// introspection globally and on each sub-tree.
//
// While doing so, it will fold numeric constants and bind placeholder names to
// their inferred types in the provided context. The optional desired parameter can
// be used to hint the desired type for the root of the resulting typed expression
// tree. Like with Expr.TypeCheck, it is not valid to provide a nil desired
// type. Instead, call it with the wildcard type TypeAny if no specific type is
// desired.
func TypeCheck(expr Expr, ctx *SemaContext, desired Type) (TypedExpr, error) {
	if desired == nil {
		panic("the desired type for parser.TypeCheck cannot be nil, use TypeAny instead")
	}
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
func TypeCheckAndRequire(expr Expr, ctx *SemaContext, required Type, op string) (TypedExpr, error) {
	typedExpr, err := TypeCheck(expr, ctx, required)
	if err != nil {
		return nil, err
	}
	if typ := typedExpr.ResolvedType(); !(typ.Equivalent(required) || typ == TypeNull) {
		return typedExpr, fmt.Errorf("argument of %s must be type %s, not type %s", op, required, typ)
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
func parse(sql string) (StatementList, error) {
	var p Parser
	return p.Parse(sql)
}

// ParseOne parses a sql statement.
func ParseOne(sql string) (Statement, error) {
	stmts, err := parse(sql)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, errors.Errorf("expected 1 statement, but found %d", len(stmts))
	}
	return stmts[0], nil
}

// ParseTableName parses a table name.
func ParseTableName(sql string) (*TableName, error) {
	stmt, err := ParseOne(fmt.Sprintf("ALTER TABLE %s RENAME TO x", sql))
	if err != nil {
		return nil, err
	}
	rename, ok := stmt.(*RenameTable)
	if !ok {
		return nil, errors.Errorf("expected an ALTER TABLE statement, but found %T", stmt)
	}
	return rename.Name.Normalize()
}

// parseExprs parses one or more sql expression.
func parseExprs(exprs []string) (Exprs, error) {
	stmt, err := ParseOne(fmt.Sprintf("SET ROW (%s)", strings.Join(exprs, ",")))
	if err != nil {
		return nil, err
	}
	set, ok := stmt.(*Set)
	if !ok {
		return nil, errors.Errorf("expected a SET statement, but found %T", stmt)
	}
	return set.Values, nil
}

// ParseExprs is a short-hand for parseExprs(sql)
func ParseExprs(sql []string) (Exprs, error) {
	if len(sql) == 0 {
		return Exprs{}, nil
	}
	return parseExprs(sql)
}

// ParseExpr is a short-hand for parseExprs([]string{sql})
func ParseExpr(sql string) (Expr, error) {
	exprs, err := parseExprs([]string{sql})
	if err != nil {
		return nil, err
	}
	if len(exprs) != 1 {
		return nil, errors.Errorf("expected 1 expression, found %d", len(exprs))
	}
	return exprs[0], nil
}

// ParseType parses a column type.
func ParseType(sql string) (CastTargetType, error) {
	expr, err := ParseExpr(fmt.Sprintf("1::%s", sql))
	if err != nil {
		return nil, err
	}

	cast, ok := expr.(*CastExpr)
	if !ok {
		return nil, errors.Errorf("expected a CastExpr, but found %T", expr)
	}

	return cast.Type, nil
}
