// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
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

package parser

import (
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// Parser wraps a scanner, parser and other utilities present in the parser
// package.
type Parser struct {
	scanner    Scanner
	parserImpl sqlParserImpl
}

// Parse parses the sql and returns a list of statements.
func (p *Parser) Parse(sql string) (stmts tree.StatementList, err error) {
	return parseWithDepth(1, sql)
}

func (p *Parser) parseWithDepth(depth int, sql string) (stmts tree.StatementList, err error) {
	p.scanner.init(sql)
	if p.parserImpl.Parse(&p.scanner) != 0 {
		var err *pgerror.Error
		if feat := p.scanner.lastError.unimplementedFeature; feat != "" {
			err = pgerror.UnimplementedWithDepth(depth+1, feat, p.scanner.lastError.msg)
		} else {
			err = pgerror.NewErrorWithDepth(depth+1, pgerror.CodeSyntaxError, p.scanner.lastError.msg)
		}
		err.Hint = p.scanner.lastError.hint
		err.Detail = p.scanner.lastError.detail
		return nil, err
	}
	return p.scanner.stmts, nil
}

// Parse parses a sql statement string and returns a list of Statements.
func Parse(sql string) (tree.StatementList, error) {
	return parseWithDepth(1, sql)
}

func parseWithDepth(depth int, sql string) (tree.StatementList, error) {
	var p Parser
	return p.parseWithDepth(depth+1, sql)
}

// ParseOne parses a sql statement string, ensuring that it contains only a
// single statement, and returns that Statement.
func ParseOne(sql string) (tree.Statement, error) {
	stmts, err := parseWithDepth(1, sql)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, pgerror.NewErrorf(
			pgerror.CodeInternalError, "expected 1 statement, but found %d", len(stmts))
	}
	return stmts[0], nil
}

// ParseTableNameWithIndex parses a table name with index.
func ParseTableNameWithIndex(sql string) (tree.TableNameWithIndex, error) {
	// We wrap the name we want to parse into a dummy statement since our parser
	// can only parse full statements.
	stmt, err := ParseOne(fmt.Sprintf("ALTER INDEX %s RENAME TO x", sql))
	if err != nil {
		return tree.TableNameWithIndex{}, err
	}
	rename, ok := stmt.(*tree.RenameIndex)
	if !ok {
		return tree.TableNameWithIndex{}, pgerror.NewErrorf(
			pgerror.CodeInternalError, "expected an ALTER INDEX statement, but found %T", stmt)
	}
	return *rename.Index, nil
}

// ParseTableName parses a table name.
func ParseTableName(sql string) (*tree.TableName, error) {
	// We wrap the name we want to parse into a dummy statement since our parser
	// can only parse full statements.
	stmt, err := ParseOne(fmt.Sprintf("ALTER TABLE %s RENAME TO x", sql))
	if err != nil {
		return nil, err
	}
	rename, ok := stmt.(*tree.RenameTable)
	if !ok {
		return nil, pgerror.NewErrorf(
			pgerror.CodeInternalError, "expected an ALTER TABLE statement, but found %T", stmt)
	}
	return rename.Name.Normalize()
}

// parseExprs parses one or more sql expressions.
func parseExprs(exprs []string) (tree.Exprs, error) {
	stmt, err := ParseOne(fmt.Sprintf("SET ROW (%s)", strings.Join(exprs, ",")))
	if err != nil {
		return nil, err
	}
	set, ok := stmt.(*tree.SetVar)
	if !ok {
		return nil, pgerror.NewErrorf(pgerror.CodeInternalError, "expected a SET statement, but found %T", stmt)
	}
	return set.Values, nil
}

// ParseExprs is a short-hand for parseExprs(sql)
func ParseExprs(sql []string) (tree.Exprs, error) {
	if len(sql) == 0 {
		return tree.Exprs{}, nil
	}
	return parseExprs(sql)
}

// ParseExpr is a short-hand for parseExprs([]string{sql})
func ParseExpr(sql string) (tree.Expr, error) {
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
func ParseType(sql string) (coltypes.CastTargetType, error) {
	expr, err := ParseExpr(fmt.Sprintf("1::%s", sql))
	if err != nil {
		return nil, err
	}

	cast, ok := expr.(*tree.CastExpr)
	if !ok {
		return nil, pgerror.NewErrorf(pgerror.CodeInternalError, "expected a tree.CastExpr, but found %T", expr)
	}

	return cast.Type, nil
}

// ParseStringAs parses s as type t.
func ParseStringAs(
	t types.T, s string, evalCtx *tree.EvalContext, env *tree.CollationEnvironment,
) (tree.Datum, error) {
	var d tree.Datum
	var err error
	switch t {
	case types.Bool:
		d, err = tree.ParseDBool(s)
	case types.Bytes:
		d = tree.NewDBytes(tree.DBytes(s))
	case types.Date:
		d, err = tree.ParseDDate(s, evalCtx.GetLocation())
	case types.Decimal:
		d, err = tree.ParseDDecimal(s)
	case types.Float:
		d, err = tree.ParseDFloat(s)
	case types.Int:
		d, err = tree.ParseDInt(s)
	case types.Interval:
		d, err = tree.ParseDInterval(s)
	case types.String:
		d = tree.NewDString(s)
	case types.Time:
		d, err = tree.ParseDTime(s)
	case types.Timestamp:
		d, err = tree.ParseDTimestamp(s, time.Microsecond)
	case types.TimestampTZ:
		d, err = tree.ParseDTimestampTZ(s, evalCtx.GetLocation(), time.Microsecond)
	case types.UUID:
		d, err = tree.ParseDUuidFromString(s)
	case types.INet:
		d, err = tree.ParseDIPAddrFromINetString(s)
	case types.JSON:
		d, err = tree.ParseDJSON(s)
	default:
		switch t := t.(type) {
		case types.TArray:
			typ, err := coltypes.DatumTypeToColumnType(t.Typ)
			if err != nil {
				return nil, err
			}
			d, err = tree.ParseDArrayFromString(evalCtx, s, typ)
			if err != nil {
				return nil, err
			}
		case types.TCollatedString:
			d = tree.NewDCollatedString(s, t.Locale, env)
		default:
			return nil, pgerror.NewErrorf(pgerror.CodeInternalError, "unknown type %s (%T)", t, t)
		}
	}
	return d, err
}
