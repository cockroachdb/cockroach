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

// This code was derived from https://github.com/youtube/vitess.
//
// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file

package parser

import (
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

//go:generate make

// Parser wraps a scanner, parser and other utilities present in the parser
// package.
type Parser struct {
	scanner    Scanner
	parserImpl sqlParserImpl
}

// Parse parses the sql and returns a list of statements.
func (p *Parser) Parse(sql string) (stmts StatementList, err error) {
	p.scanner.init(sql)
	if p.parserImpl.Parse(&p.scanner) != 0 {
		var err *pgerror.Error
		if feat := p.scanner.lastError.unimplementedFeature; feat != "" {
			err = pgerror.Unimplemented(feat, p.scanner.lastError.msg)
		} else {
			err = pgerror.NewError(pgerror.CodeSyntaxError, p.scanner.lastError.msg)
		}
		err.Hint = p.scanner.lastError.hint
		err.Detail = p.scanner.lastError.detail
		return nil, err
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
// type. Instead, call it with the wildcard type types.Any if no specific type is
// desired.
func TypeCheck(expr Expr, ctx *SemaContext, desired types.T) (TypedExpr, error) {
	if desired == nil {
		panic("the desired type for parser.TypeCheck cannot be nil, use types.Any instead")
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
func TypeCheckAndRequire(
	expr Expr, ctx *SemaContext, required types.T, op string,
) (TypedExpr, error) {
	typedExpr, err := TypeCheck(expr, ctx, required)
	if err != nil {
		return nil, err
	}
	if typ := typedExpr.ResolvedType(); !(typ.Equivalent(required) || typ == types.Null) {
		return typedExpr, pgerror.NewErrorf(
			pgerror.CodeDatatypeMismatchError, "argument of %s must be type %s, not type %s", op, required, typ)
	}
	return typedExpr, nil
}

// Parse parses a sql statement string and returns a list of Statements.
func Parse(sql string) (StatementList, error) {
	var p Parser
	return p.Parse(sql)
}

// ParseOne parses a sql statement string, ensuring that it contains only a
// single statement, and returns that Statement.
func ParseOne(sql string) (Statement, error) {
	stmts, err := Parse(sql)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, pgerror.NewErrorf(
			pgerror.CodeInternalError, "expected 1 statement, but found %d", len(stmts))
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
		return nil, pgerror.NewErrorf(pgerror.CodeInternalError, "expected an ALTER TABLE statement, but found %T", stmt)
	}
	return rename.Name.Normalize()
}

// parseExprs parses one or more sql expressions.
func parseExprs(exprs []string) (Exprs, error) {
	stmt, err := ParseOne(fmt.Sprintf("SET ROW (%s)", strings.Join(exprs, ",")))
	if err != nil {
		return nil, err
	}
	set, ok := stmt.(*SetVar)
	if !ok {
		return nil, pgerror.NewErrorf(pgerror.CodeInternalError, "expected a SET statement, but found %T", stmt)
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
		return nil, pgerror.NewErrorf(pgerror.CodeInternalError, "expected 1 expression, found %d", len(exprs))
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
		return nil, pgerror.NewErrorf(pgerror.CodeInternalError, "expected a CastExpr, but found %T", expr)
	}

	return cast.Type, nil
}

// ParseStringAs parses s as type t.
func ParseStringAs(t types.T, s string, evalCtx *EvalContext) (Datum, error) {
	var d Datum
	var err error
	switch t {
	case types.Bool:
		d, err = ParseDBool(s)
	case types.Bytes:
		d = NewDBytes(DBytes(s))
	case types.Date:
		d, err = ParseDDate(s, evalCtx.GetLocation())
	case types.Decimal:
		d, err = ParseDDecimal(s)
	case types.Float:
		d, err = ParseDFloat(s)
	case types.Int:
		d, err = ParseDInt(s)
	case types.Interval:
		d, err = ParseDInterval(s)
	case types.String:
		d = NewDString(s)
	case types.Timestamp:
		d, err = ParseDTimestamp(s, time.Microsecond)
	case types.TimestampTZ:
		d, err = ParseDTimestampTZ(s, evalCtx.GetLocation(), time.Microsecond)
	case types.UUID:
		d, err = ParseDUuidFromString(s)
	case types.INet:
		d, err = ParseDIPAddrFromINetString(s)
	default:
		if a, ok := t.(types.TArray); ok {
			typ, err := DatumTypeToColumnType(a.Typ)
			if err != nil {
				return nil, err
			}
			d, err = ParseDArrayFromString(evalCtx, s, typ)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, pgerror.NewErrorf(pgerror.CodeInternalError, "unknown type %s", t)
		}
	}
	return d, err
}
