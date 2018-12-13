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

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// Parser wraps a scanner, parser and other utilities present in the parser
// package.
type Parser struct {
	scanner    scanner
	lexer      lexer
	parserImpl sqlParserImpl
	tokBuf     [8]sqlSymType
	stmtBuf    [1]tree.Statement
	strBuf     [1]string
}

// INT8 is the historical interpretation of INT. This should be left
// alone in the future, since there are many sql fragments stored
// in various descriptors.  Any user input that was created after
// INT := INT4 will simply use INT4 in any resulting code.
var defaultNakedIntType = coltypes.Int8
var defaultNakedSerialType = coltypes.Serial8

// Parse parses the sql and returns a list of statements.
func (p *Parser) Parse(sql string) (stmts tree.StatementList, sqlStrings []string, _ error) {
	return p.parseWithDepth(1, sql, defaultNakedIntType, defaultNakedSerialType)
}

// ParseWithInt parses a sql statement string and returns a list of
// Statements. The INT token will result in the specified TInt type.
func (p *Parser) ParseWithInt(
	sql string, nakedIntType *coltypes.TInt,
) (stmts tree.StatementList, sqlStrings []string, _ error) {
	nakedSerialType := coltypes.Serial8
	if nakedIntType == coltypes.Int4 {
		nakedSerialType = coltypes.Serial4
	}
	return p.parseWithDepth(1, sql, nakedIntType, nakedSerialType)
}

func (p *Parser) parseOneWithDepth(depth int, sql string) (tree.Statement, error) {
	stmts, _, err := p.parseWithDepth(1, sql, defaultNakedIntType, defaultNakedSerialType)
	if err != nil {
		return nil, err
	}
	if len(stmts) != 1 {
		return nil, pgerror.NewAssertionErrorf("expected 1 statement, but found %d", len(stmts))
	}
	return stmts[0], nil
}

func (p *Parser) scanOneStmt() (sql string, tokens []sqlSymType, done bool) {
	var lval sqlSymType
	tokens = p.tokBuf[:0]

	// Scan the first token.
	for {
		p.scanner.scan(&lval)
		if lval.id == 0 {
			return "", nil, true
		}
		if lval.id != ';' {
			break
		}
	}

	startPos := lval.pos
	// We make the resulting token positions match the returned string.
	lval.pos = 0
	tokens = append(tokens, lval)
	for {
		if lval.id == ERROR {
			return p.scanner.in[startPos:], tokens, true
		}
		posBeforeScan := p.scanner.pos
		p.scanner.scan(&lval)
		if lval.id == 0 || lval.id == ';' {
			return p.scanner.in[startPos:posBeforeScan], tokens, (lval.id == 0)
		}
		lval.pos -= startPos
		tokens = append(tokens, lval)
	}
}

func (p *Parser) parseWithDepth(
	depth int, sql string, nakedIntType *coltypes.TInt, nakedSerialType *coltypes.TSerial,
) (stmts tree.StatementList, sqlStrings []string, err error) {
	stmts = tree.StatementList(p.stmtBuf[:0])
	sqlStrings = p.strBuf[:0]
	p.scanner.init(sql)
	defer p.scanner.cleanup()
	for {
		sql, tokens, done := p.scanOneStmt()
		stmt, err := p.parse(depth+1, sql, tokens, nakedIntType, nakedSerialType)
		if err != nil {
			return nil, nil, err
		}
		if stmt != nil {
			stmts = append(stmts, stmt)
			sqlStrings = append(sqlStrings, sql)
		}
		if done {
			break
		}
	}
	return stmts, sqlStrings, nil
}

// parse parses a statement from the given scanned tokens.
func (p *Parser) parse(
	depth int,
	sql string,
	tokens []sqlSymType,
	nakedIntType *coltypes.TInt,
	nakedSerialType *coltypes.TSerial,
) (tree.Statement, error) {
	p.lexer.init(sql, tokens, nakedIntType, nakedSerialType)
	defer p.lexer.cleanup()
	if p.parserImpl.Parse(&p.lexer) != 0 {
		lastError := p.lexer.lastError
		var err *pgerror.Error
		if feat := lastError.unimplementedFeature; feat != "" {
			// UnimplementedWithDepth populates the generic hint. However
			// in some cases we have a more specific hint. This is overridden
			// below.
			err = pgerror.UnimplementedWithDepth(depth+1, "syntax."+feat, lastError.msg)
		} else {
			err = pgerror.NewErrorWithDepth(depth+1, pgerror.CodeSyntaxError, lastError.msg)
		}
		if lastError.hint != "" {
			// If lastError.hint is not set, e.g. from (*scanner).Unimplemented(),
			// we're OK with the default hint. Otherwise, override it.
			err.Hint = lastError.hint
		}
		err.Detail = lastError.detail
		return nil, err
	}
	return p.lexer.stmt, nil
}

// unaryNegation constructs an AST node for a negation. This attempts
// to preserve constant NumVals and embed the negative sign inside
// them instead of wrapping in an UnaryExpr. This in turn ensures
// that negative numbers get considered as a single constant
// for the purpose of formatting and scrubbing.
func unaryNegation(e tree.Expr) tree.Expr {
	if cst, ok := e.(*tree.NumVal); ok {
		cst.Negative = !cst.Negative
		return cst
	}

	// Common case.
	return &tree.UnaryExpr{Operator: tree.UnaryMinus, Expr: e}
}

// Parse parses a sql statement string and returns a list of Statements.
func Parse(sql string) (stmts tree.StatementList, sqlStrings []string, _ error) {
	var p Parser
	return p.parseWithDepth(1, sql, defaultNakedIntType, defaultNakedSerialType)
}

// ParseOne parses a sql statement string, ensuring that it contains only a
// single statement, and returns that Statement. ParseOne will always
// interpret the INT and SERIAL types as 64-bit types, since this is
// used in various internal-execution paths where we might receive
// bits of SQL from other nodes. In general, we expect that all
// user-generated SQL has been run through the ParseWithInt() function.
func ParseOne(sql string) (tree.Statement, error) {
	var p Parser
	return p.parseOneWithDepth(1, sql)
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
		return tree.TableNameWithIndex{}, pgerror.NewAssertionErrorf("expected an ALTER INDEX statement, but found %T", stmt)
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
		return nil, pgerror.NewAssertionErrorf("expected an ALTER TABLE statement, but found %T", stmt)
	}
	return &rename.Name, nil
}

// parseExprs parses one or more sql expressions.
func parseExprs(exprs []string) (tree.Exprs, error) {
	stmt, err := ParseOne(fmt.Sprintf("SET ROW (%s)", strings.Join(exprs, ",")))
	if err != nil {
		return nil, err
	}
	set, ok := stmt.(*tree.SetVar)
	if !ok {
		return nil, pgerror.NewAssertionErrorf("expected a SET statement, but found %T", stmt)
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
		return nil, pgerror.NewAssertionErrorf("expected 1 expression, found %d", len(exprs))
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
		return nil, pgerror.NewAssertionErrorf("expected a tree.CastExpr, but found %T", expr)
	}

	return cast.Type, nil
}
